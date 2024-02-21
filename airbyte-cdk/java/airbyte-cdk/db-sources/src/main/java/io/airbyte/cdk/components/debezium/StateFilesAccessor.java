/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.components.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.jackson.MoreMappers;
import io.debezium.config.Configuration;
import io.debezium.relational.history.AbstractFileBasedSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.file.history.FileSchemaHistory;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;

public class StateFilesAccessor implements AutoCloseable {

  final public Path workingDir;
  final public Path offsetFilePath;
  final private FileOffsetBackingStore fileOffsetBackingStore;
  final public Path schemaFilePath;
  final private FileSchemaHistory fileSchemaHistory;

  // Use reflection to access the necessary protected methods in FileSchemaHistory.
  static private final Method STORE_RECORD_METHOD;
  static private final Method RECOVER_RECORDS_METHOD;

  static {
    try {
      STORE_RECORD_METHOD = AbstractFileBasedSchemaHistory.class.getDeclaredMethod("storeRecord", HistoryRecord.class);
      RECOVER_RECORDS_METHOD = AbstractFileBasedSchemaHistory.class.getDeclaredMethod("recoverRecords", Consumer.class);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
    STORE_RECORD_METHOD.setAccessible(true);
    RECOVER_RECORDS_METHOD.setAccessible(true);
  }

  public StateFilesAccessor() {
    try {
      workingDir = Files.createTempDirectory(Path.of("/tmp"), "airbyte-debezium-state");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    offsetFilePath = workingDir.resolve("offset.dat");
    schemaFilePath = workingDir.resolve("dbhistory.dat");
    // Create and configure FileOffsetBackingStore instance.
    fileOffsetBackingStore = new FileOffsetBackingStore(keyConverter());
    var offsetProps = new HashMap<String, String>();
    offsetProps.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    offsetProps.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    offsetProps.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, offsetFilePath.toString());
    fileOffsetBackingStore.configure(new StandaloneConfig(offsetProps));
    // Create and configure FileSchemaHistory instance.
    fileSchemaHistory = new FileSchemaHistory();
    var schemaConfig = Configuration.create()
        .with(FileSchemaHistory.FILE_PATH, schemaFilePath.toString())
        .build();
    fileSchemaHistory.configure(schemaConfig, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
  }

  @Override
  public void close() {
    fileOffsetBackingStore.stop();
    fileSchemaHistory.stop();
    try {
      FileUtils.deleteDirectory(workingDir.toFile());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public DebeziumComponent.State.Offset readUpdatedOffset(DebeziumComponent.State.Offset offset) {
    var keys = offset.debeziumOffset().keySet().stream().map(StateFilesAccessor::toByteBuffer).toList();
    fileOffsetBackingStore.start();
    var future = fileOffsetBackingStore.get(keys);
    fileOffsetBackingStore.stop();
    var map = future.resultNow();
    var builder = ImmutableMap.<JsonNode, JsonNode>builder();
    for (var e : map.entrySet()) {
      builder.put(toJson(e.getKey()), toJson(e.getValue()));
    }
    return new DebeziumComponent.State.Offset(builder.build());
  }

  public void writeOffset(DebeziumComponent.State.Offset offset) {
    var map = new HashMap<ByteBuffer, ByteBuffer>(offset.debeziumOffset().size());
    for (var e : offset.debeziumOffset().entrySet()) {
      map.put(toByteBuffer(e.getKey()), toByteBuffer(e.getValue()));
    }
    fileOffsetBackingStore.start();
    fileOffsetBackingStore.set(map, null);
    fileOffsetBackingStore.stop();
  }

  public DebeziumComponent.State.Schema readSchema() {
    var builder = ImmutableList.<HistoryRecord>builder();
    Consumer<HistoryRecord> consumer = builder::add;
    fileSchemaHistory.start();
    try {
      RECOVER_RECORDS_METHOD.invoke(fileSchemaHistory, consumer);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
    fileSchemaHistory.stop();
    return new DebeziumComponent.State.Schema(builder.build());
  }

  public void writeSchema(DebeziumComponent.State.Schema schema) {
    fileSchemaHistory.initializeStorage();
    fileSchemaHistory.start();
    for (var r : schema.debeziumSchemaHistory()) {
      try {
        STORE_RECORD_METHOD.invoke(fileSchemaHistory, r);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
    fileSchemaHistory.stop();
  }

  private Converter keyConverter() {
    var c = new JsonConverter();
    c.configure(INTERNAL_CONVERTER_CONFIG, true);
    return c;
  }

  static private final Map<String, String> INTERNAL_CONVERTER_CONFIG = Map.of(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, Boolean.FALSE.toString());

  static private JsonNode toJson(final ByteBuffer byteBuffer) {
    try {
      return OBJECT_MAPPER.readTree(new ByteBufferInputStream(byteBuffer.asReadOnlyBuffer()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static private ByteBuffer toByteBuffer(final JsonNode json) {
    try {
      return ByteBuffer.wrap(OBJECT_MAPPER.writeValueAsBytes(json));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static private final ObjectMapper OBJECT_MAPPER = MoreMappers.initMapper();

}
