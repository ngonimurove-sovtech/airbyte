/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.components.debezium;

import com.google.common.collect.ImmutableList;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class DebeziumEngineSharedState {

  private final AtomicLong startEpochMilli = new AtomicLong();
  private final AtomicLong numRecords = new AtomicLong();
  private final AtomicLong lastRecordEpochMilli = new AtomicLong();
  private final AtomicLong completionReasonsBitfield = new AtomicLong();
  private final List<BufferElement> buffer = Collections.synchronizedList(new ArrayList<>(1_000_000));
  private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

  private record BufferElement(DebeziumComponent.Output.Record record, boolean isWithinBounds, long epochMilli) {}

  public DebeziumEngineSharedState() {}

  public void reset() {
    startEpochMilli.set(Instant.now().toEpochMilli());
    numRecords.set(0L);
    completionReasonsBitfield.set(0L);
    lastRecordEpochMilli.set(startEpochMilli.get());
    synchronized (buffer) {
      buffer.clear();
    }
  }

  public boolean isComplete() {
    return completionReasonsBitfield.get() != 0L;
  }

  public void add(DebeziumComponent.Output.Record record, DebeziumComponent.Input.CompletionTargets targets) {
    // Store current state before updating it.
    final var now = Instant.now();
    final long previousNumRecords = numRecords();
    final var elapsedSinceStart = Duration.between(startedAt(), now);
    final var elapsedSinceLastRecord = Duration.between(lastRecordAt(), now);
    final boolean isWithinBounds = targets.isWithinBounds().test(record);
    // Update buffer.
    addToBuffer(new BufferElement(record, isWithinBounds, now.toEpochMilli()));
    // Time-based completion checks.
    if (targets.maxTime().minus(elapsedSinceStart).isNegative()) {
      // We have spent enough time collecting records, shut down.
      addCompletionReason(DebeziumComponent.Output.CompletionReason.HAS_COLLECTED_LONG_ENOUGH);
    }
    if (previousNumRecords == 0) {
      if (targets.maxWaitTimeForFirstRecord().minus(elapsedSinceLastRecord).isNegative()) {
        // We have spend enough time waiting for the first record, shut down.
        addCompletionReason(DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_INITIAL_RECORD);
      }
    } else {
      if (targets.maxWaitTimeForSubsequentRecord().minus(elapsedSinceLastRecord).isNegative()) {
        // We have spend enough time waiting for a subsequent record, shut down.
        addCompletionReason(DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD);
      }
    }
    // Other engine completion checks.
    if (!isWithinBounds) {
      // We exceeded the high-water mark, shut down.
      addCompletionReason(DebeziumComponent.Output.CompletionReason.HAS_EVENTS_OUT_OF_BOUNDS);
    }
    if (!record.isHeartbeat() && previousNumRecords + 1 >= targets.maxRecords()) {
      // We have collected enough records, shut down.
      addCompletionReason(DebeziumComponent.Output.CompletionReason.HAS_COLLECTED_ENOUGH_RECORDS);
    }
    if (record.kind() == DebeziumComponent.Output.Record.Kind.SNAPSHOT_COMPLETE) {
      // We were snapshotting and we finished the snapshot, shut down.
      addCompletionReason(DebeziumComponent.Output.CompletionReason.HAS_FINISHED_SNAPSHOTTING);
    }
    if (previousNumRecords % 1000 == 0) {
      // Don't check memory usage too often, there is some overhead, even if it's not a lot.
      var heapUsage = memoryBean.getHeapMemoryUsage();
      double maxHeapUsageRatio = Math.min(1.0, Math.max(0.0, 1.0 - targets.minUnusedHeapRatio()));
      if (heapUsage.getUsed() > heapUsage.getMax() * maxHeapUsageRatio) {
        // We are using more memory than we'd like, shut down.
        addCompletionReason(DebeziumComponent.Output.CompletionReason.MEMORY_PRESSURE);
      }
    }

  }

  private long numRecords() {
    return numRecords.get();
  }

  private Instant startedAt() {
    return Instant.ofEpochMilli(startEpochMilli.get());
  }

  private Instant lastRecordAt() {
    return Instant.ofEpochMilli(lastRecordEpochMilli.get());
  }

  private void addToBuffer(BufferElement e) {
    synchronized (buffer) {
      buffer.add(e);
    }
    if (!e.record.isHeartbeat()) {
      numRecords.getAndIncrement();
      lastRecordEpochMilli.getAndUpdate(acc -> Long.max(acc, e.epochMilli));
    }
  }

  public void addCompletionReason(DebeziumComponent.Output.CompletionReason reason) {
    completionReasonsBitfield.getAndUpdate(acc -> acc | 1L << reason.ordinal());
  }

  public DebeziumComponent.Output build(DebeziumComponent.State state) {
    var records = ImmutableList.<DebeziumComponent.Output.Record>builderWithExpectedSize((int) numRecords.get());
    var eventStats = new DescriptiveStatistics();
    var recordStats = new DescriptiveStatistics();
    var recordOutOfBoundsStats = new DescriptiveStatistics();
    synchronized (buffer) {
      var previousEventEpochMilli = startEpochMilli.get();
      var previousRecordEpochMilli = startEpochMilli.get();
      var previousRecordOutOfBoundsEpochMilli = startEpochMilli.get();
      for (var e : buffer) {
        eventStats.addValue(Math.max(0.0, e.epochMilli - previousEventEpochMilli));
        previousEventEpochMilli = e.epochMilli;
        if (!e.record.isHeartbeat()) {
          records.add(e.record);
          recordStats.addValue(Math.max(0.0, e.epochMilli - previousRecordEpochMilli));
          previousRecordEpochMilli = e.epochMilli;
          if (!e.isWithinBounds) {
            recordOutOfBoundsStats.addValue(Math.max(0.0, e.epochMilli - previousRecordOutOfBoundsEpochMilli));
            previousRecordOutOfBoundsEpochMilli = e.epochMilli;
          }
        }
      }
    }
    final var completionReasons = EnumSet.noneOf(DebeziumComponent.Output.CompletionReason.class);
    for (var reason : DebeziumComponent.Output.CompletionReason.values()) {
      if ((completionReasonsBitfield.get() & (1L << reason.ordinal())) != 0L) {
        completionReasons.add(reason);
      }
    }
    return new DebeziumComponent.Output(
        records.build(),
        state,
        new DebeziumComponent.Output.ExecutionSummary(
            new DebeziumComponent.Output.ExecutionSummary.LatencyStats(eventStats),
            new DebeziumComponent.Output.ExecutionSummary.LatencyStats(recordStats),
            new DebeziumComponent.Output.ExecutionSummary.LatencyStats(recordOutOfBoundsStats),
            Duration.ofMillis(Instant.now().toEpochMilli() - startEpochMilli.get())),
        completionReasons);
  }

}
