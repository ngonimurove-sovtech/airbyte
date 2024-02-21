/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.components.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.debezium.relational.history.HistoryRecord;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.kafka.connect.source.SourceRecord;

public interface DebeziumComponent {

  Output collect(Input input);

  record Input(Config config, State state, CompletionTargets completionTargets) {

    public record Config(Properties properties) {

      String debeziumName() {
        return properties.getProperty("name");
      }

    }

    public record CompletionTargets(
                                    Predicate<Output.Record> isWithinBounds,
                                    long maxRecords,
                                    Duration maxTime,
                                    Duration maxWaitTimeForFirstRecord,
                                    Duration maxWaitTimeForSubsequentRecord,
                                    double minUnusedHeapRatio) {}

  }

  record State(Offset offset, Optional<Schema> schema) {

    public record Offset(Map<JsonNode, JsonNode> debeziumOffset) {}

    public record Schema(List<HistoryRecord> debeziumSchemaHistory) {}

  }

  record Output(
                Collection<Record> data,
                State state,
                ExecutionSummary executionSummary,
                Set<CompletionReason> completionReasons) {

    public record Record(JsonNode debeziumEventValue, Optional<SourceRecord> debeziumSourceRecord) {

      public boolean isHeartbeat() {
        return kind() == Kind.HEARTBEAT;
      }

      public JsonNode before() {
        return element("before");
      }

      public JsonNode after() {
        return element("after");
      }

      public JsonNode source() {
        return element("source");
      }

      public JsonNode element(String fieldName) {
        if (!debeziumEventValue.has(fieldName)) {
          return NullNode.getInstance();
        }
        return debeziumEventValue.get(fieldName);
      }

      public Kind kind() {
        var source = source();
        if (source.isNull()) {
          return Kind.HEARTBEAT;
        }
        var snapshot = source.get("snapshot");
        if (snapshot == null) {
          return Kind.CHANGE;
        }
        return switch (snapshot.asText().toLowerCase()) {
          case "false" -> Kind.CHANGE;
          case "last" -> Kind.SNAPSHOT_COMPLETE;
          default -> Kind.SNAPSHOT_ONGOING;
        };
      }

      public enum Kind {
        HEARTBEAT,
        CHANGE,
        SNAPSHOT_ONGOING,
        SNAPSHOT_COMPLETE,
      }

    }

    public record ExecutionSummary(
                                   LatencyStats events,
                                   LatencyStats records,
                                   LatencyStats recordsOutOfBounds,
                                   Duration collectDuration) {

      public record LatencyStats(DescriptiveStatistics elapsedSincePreviousMilli) {

        public long count() {
          return elapsedSincePreviousMilli.getN();
        }

        public Duration first() {
          return durationStat(elapsedSincePreviousMilli.getElement(0));
        }

        public Duration last() {
          return durationStat(elapsedSincePreviousMilli.getElement((int) count() - 1));
        }

        public Duration min() {
          return durationStat(elapsedSincePreviousMilli.getMin());
        }

        public Duration p01() {
          return durationStat(elapsedSincePreviousMilli.getPercentile(0.01));
        }

        public Duration p05() {
          return durationStat(elapsedSincePreviousMilli.getPercentile(0.05));
        }

        public Duration p10() {
          return durationStat(elapsedSincePreviousMilli.getPercentile(0.10));
        }

        public Duration p25() {
          return durationStat(elapsedSincePreviousMilli.getPercentile(0.25));
        }

        public Duration median() {
          return durationStat(elapsedSincePreviousMilli.getPercentile(0.50));
        }

        public Duration p75() {
          return durationStat(elapsedSincePreviousMilli.getPercentile(0.75));
        }

        public Duration p90() {
          return durationStat(elapsedSincePreviousMilli.getPercentile(0.90));
        }

        public Duration p95() {
          return durationStat(elapsedSincePreviousMilli.getPercentile(0.95));
        }

        public Duration p99() {
          return durationStat(elapsedSincePreviousMilli.getPercentile(0.99));
        }

        public Duration max() {
          return durationStat(elapsedSincePreviousMilli.getMax());
        }

        private Duration durationStat(double stat) {
          return Duration.ofMillis((long) stat);
        }

      }

    }

    public enum CompletionReason {
      HAS_FINISHED_SNAPSHOTTING,
      HAS_EVENTS_OUT_OF_BOUNDS,
      HAS_COLLECTED_ENOUGH_RECORDS,
      HAS_COLLECTED_LONG_ENOUGH,
      HAS_WAITED_LONG_ENOUGH_FOR_INITIAL_RECORD,
      HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD,
      MEMORY_PRESSURE,
    }

  }

}
