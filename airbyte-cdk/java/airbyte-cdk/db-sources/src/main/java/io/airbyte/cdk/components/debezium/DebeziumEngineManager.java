/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.components.debezium;

import io.airbyte.commons.json.Jsons;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.storage.file.history.FileSchemaHistory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumEngineManager {

  static private final Logger LOGGER = LoggerFactory.getLogger(DebeziumEngineManager.class);
  static private final Class<?> EMBEDDED_ENGINE_CHANGE_EVENT;
  static private final Method SOURCE_RECORD_GETTER;

  static {
    try {
      EMBEDDED_ENGINE_CHANGE_EVENT = Class.forName("io.debezium.embedded.EmbeddedEngineChangeEvent");
      SOURCE_RECORD_GETTER = EMBEDDED_ENGINE_CHANGE_EVENT.getDeclaredMethod("sourceRecord");
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
    SOURCE_RECORD_GETTER.setAccessible(true);
  }

  // Constructor args.
  public final DebeziumComponent.Input input;
  private final StateFilesAccessor stateFilesAccessor;

  // Shared state.
  private final DebeziumEngineSharedState data = new DebeziumEngineSharedState();

  // Engine running and stopping.
  private final DebeziumEngine<ChangeEvent<String, String>> engine;
  private final ExecutorService engineExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "debezium-engine-runner"));
  private final AtomicReference<Throwable> thrown = new AtomicReference<>();
  private final AtomicBoolean isStopping = new AtomicBoolean();
  private final ExecutorService stopperExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "debezium-engine-stopper"));

  public DebeziumEngineManager(DebeziumComponent.Input input, StateFilesAccessor stateFilesAccessor) {
    this.input = input;
    this.stateFilesAccessor = stateFilesAccessor;
    var props = (Properties) input.config().properties().clone();
    props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, stateFilesAccessor.offsetFilePath.toString());
    if (input.state().schema().isPresent()) {
      props.setProperty(FileSchemaHistory.FILE_PATH.name(), stateFilesAccessor.schemaFilePath.toString());
    }
    this.engine = DebeziumEngine.create(Json.class)
        .using(props)
        .notifying(this::onEvent)
        .using(this::onCompletion)
        .build();
  }

  private void onEvent(ChangeEvent<String, String> event) {
    if (event.value() == null) {
      // Debezium outputs a tombstone event that has a value of null. This is an artifact of how it
      // interacts with kafka. We want to ignore it. More on the tombstone:
      // https://debezium.io/documentation/reference/2.2/transformations/event-flattening.html
      return;
    }
    // Update the shared state with the event.
    data.add(recordFromEvent(event), input.completionTargets());
    // If we're done, shut down engine, at most once.
    if (data.isComplete() && isStopping.compareAndSet(false, true)) {
      // Shutting down the engine must be done in a different thread than the engine's run() method.
      // We're in the event callback right now, so that might be the current thread, we don't know.
      stopperExecutor.execute(() -> {
        try {
          engine.close();
        } catch (IOException | RuntimeException e) {
          // The close() method implementation in EmbeddedEngine is quite straightforward.
          // It delegates to stop() and its contract is to be non-blocking.
          // It doesn't throw any exceptions that we might want to act upon or propagate,
          // but let's log them just in case.
          LOGGER.warn("Exception thrown while stopping the Debezium engine.", e);
        }
      });
    }
  }

  private DebeziumComponent.Output.Record recordFromEvent(ChangeEvent<String, String> event) {
    // Deserialize the event value.
    var deserializedEventValue = Jsons.deserialize(event.value());
    // Try to get the source record.
    SourceRecord sourceRecord = null;
    if (EMBEDDED_ENGINE_CHANGE_EVENT.isInstance(event)) {
      try {
        var object = SOURCE_RECORD_GETTER.invoke(event);
        if (object instanceof SourceRecord) {
          sourceRecord = (SourceRecord) object;
        }
      } catch (IllegalAccessException | InvocationTargetException e) {
        LOGGER.debug("Failed to extract SourceRecord instance from Debezium ChangeEvent instance", e);
      }
    }
    return new DebeziumComponent.Output.Record(deserializedEventValue, Optional.ofNullable(sourceRecord));
  }

  private void onCompletion(boolean success, String message, Throwable error) {
    LOGGER.info("Debezium engine shutdown. Engine terminated successfully : {}", success);
    LOGGER.info(message);
    if (!success) {
      if (error == null) {
        // There are cases where Debezium doesn't succeed but only fills the message field.
        // In that case, we still want to fail loud and clear
        error = new RuntimeException(message);
      }
      thrown.set(error);
    }
  }

  public DebeziumEngineManager start() {
    stateFilesAccessor.writeOffset(input.state().offset());
    input.state().schema().ifPresent(stateFilesAccessor::writeSchema);
    data.reset();
    engineExecutor.execute(engine);
    return this;
  }

  public DebeziumComponent.Output await() {
    // Wait for a predetermined amount of time for the Debezium engine to finish its execution.
    engineExecutor.shutdown();
    boolean engineHasShutDown;
    try {
      engineHasShutDown = engineExecutor.awaitTermination(input.completionTargets().maxTime().toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    if (!engineHasShutDown) {
      // Engine hasn't shut down yet.
      // We've waited long enough: trigger the shutdown.
      data.addCompletionReason(DebeziumComponent.Output.CompletionReason.HAS_COLLECTED_LONG_ENOUGH);
      try {
        engine.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    // At this point, the engine has either already shut down,
    // or the shut down has already been triggered.
    stopperExecutor.shutdown();
    stopperExecutor.close();
    engineExecutor.close();
    // Re-throw any exception.
    if (thrown.get() != null) {
      throw new RuntimeException(thrown.get());
    }
    // Generate final state from file contents.
    var finalState = new DebeziumComponent.State(
        stateFilesAccessor.readUpdatedOffset(input.state().offset()),
        input.state().schema().map(__ -> stateFilesAccessor.readSchema()));
    return data.build(finalState);
  }

  static public DebeziumComponent debeziumComponent() {
    return input -> {
      try (var stateFilesManager = new StateFilesAccessor()) {
        return new DebeziumEngineManager(input, stateFilesManager).start().await();
      }
    };
  }

}
