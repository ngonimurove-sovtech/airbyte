/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres.debezium;

import com.google.common.collect.Lists;
import io.airbyte.cdk.components.debezium.DebeziumComponent;
import io.airbyte.cdk.components.debezium.DebeziumEngineManager;
import io.airbyte.cdk.components.debezium.RelationalConfigBuilder;
import io.airbyte.cdk.db.PgLsn;
import io.airbyte.commons.json.Jsons;
import io.airbyte.integrations.source.postgres.PostgresTestDatabase;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.v0.AirbyteCatalog;
import io.airbyte.protocol.models.v0.CatalogHelpers;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.SyncMode;
import io.debezium.connector.postgresql.PostgresConnector;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebeziumComponentPostgresIntegrationTest {

  PostgresTestDatabase testdb;

  static final String CREATE_TABLE_KV = """
                                        CREATE TABLE kv (k SERIAL PRIMARY KEY, v VARCHAR(60));
                                        """;

  static final String CREATE_TABLE_EVENTLOG =
      """
      CREATE TABLE eventlog (id UUID GENERATED ALWAYS AS (MD5(entry)::UUID) STORED, entry VARCHAR(60) NOT NULL);
      ALTER TABLE eventlog REPLICA IDENTITY FULL;
      """;

  AirbyteCatalog catalog() {
    return new AirbyteCatalog().withStreams(List.of(
        CatalogHelpers.createAirbyteStream(
            "kv",
            "public",
            Field.of("k", JsonSchemaType.INTEGER),
            Field.of("v", JsonSchemaType.STRING))
            .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
            .withSourceDefinedPrimaryKey(List.of(List.of("v"))),
        CatalogHelpers.createAirbyteStream(
            "eventlog",
            "public",
            Field.of("id", JsonSchemaType.STRING),
            Field.of("entry", JsonSchemaType.STRING))
            .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL))
            .withSourceDefinedPrimaryKey(List.of(List.of("id")))));
  }

  ConfiguredAirbyteCatalog configuredCatalog() {
    var configuredCatalog = CatalogHelpers.toDefaultConfiguredCatalog(catalog());
    configuredCatalog.getStreams().forEach(s -> s.setSyncMode(SyncMode.INCREMENTAL));
    return configuredCatalog;
  }

  record Change(Table table, Value oldValue, Value newValue) {

    enum Table {

      KV("v"),
      EVENTLOG("entry");

      public final String valueColumnName;

      Table(String valueColumnName) {
        this.valueColumnName = valueColumnName;
      }

    }

    enum Value {
      FOO,
      BAR,
      BAZ,
      QUUX,
      XYZZY
    }

    enum Kind {
      INSERT,
      UPDATE,
      DELETE
    }

    Kind kind() {
      if (oldValue() == null) {
        return Kind.INSERT;
      }
      if (newValue() == null) {
        return Kind.DELETE;
      }
      return Kind.UPDATE;
    }

    String sql() {
      return switch (kind()) {
        case INSERT -> String.format("INSERT INTO %s (%s) VALUES ('%s');", table(), table().valueColumnName, newValue());
        case DELETE -> String.format("DELETE FROM %s WHERE %s = '%s';", table(), table().valueColumnName, oldValue());
        case UPDATE -> String.format("UPDATE %s SET %s = '%s' WHERE %s = '%s';", table(), table().valueColumnName, newValue(),
            table().valueColumnName, oldValue());
      };
    }

    static Change insert(Table table, Value newValue) {
      return new Change(table, null, newValue);
    }

    static Change delete(Table table, Value oldValue) {
      return new Change(table, oldValue, null);
    }

    static Change update(Table table, Value oldValue, Value newValue) {
      return new Change(table, oldValue, newValue);
    }

  }

  static final List<Change> INITIAL_INSERT = List.of(
      Change.insert(Change.Table.KV, Change.Value.FOO),
      Change.insert(Change.Table.KV, Change.Value.BAR),
      Change.insert(Change.Table.EVENTLOG, Change.Value.FOO),
      Change.insert(Change.Table.EVENTLOG, Change.Value.BAR));

  static final List<Change> UPDATE_KV = List.of(
      Change.update(Change.Table.KV, Change.Value.FOO, Change.Value.QUUX),
      Change.update(Change.Table.KV, Change.Value.BAR, Change.Value.XYZZY));

  static final List<Change> DELETE_EVENTLOG = List.of(
      Change.delete(Change.Table.EVENTLOG, Change.Value.FOO),
      Change.delete(Change.Table.EVENTLOG, Change.Value.BAR));

  static final List<Change> SUBSEQUENT_INSERT = List.of(
      Change.insert(Change.Table.KV, Change.Value.BAZ),
      Change.insert(Change.Table.EVENTLOG, Change.Value.BAZ));

  @BeforeEach
  void setup() {
    testdb = PostgresTestDatabase.in(PostgresTestDatabase.BaseImage.POSTGRES_16, PostgresTestDatabase.ContainerModifier.CONF)
        .with(CREATE_TABLE_KV)
        .with(CREATE_TABLE_EVENTLOG)
        .withReplicationSlot()
        .withPublicationForAllTables()
        .with("CHECKPOINT");
  }

  @AfterEach
  void tearDown() {
    if (testdb != null) {
      testdb.close();
      testdb = null;
    }
  }

  @Test
  public void testNoProgress() {
    INITIAL_INSERT.stream().map(Change::sql).forEach(testdb::with);
    testdb.with("CHECKPOINT");
    var input1 = new DebeziumComponent.Input(config(), state(), completionTargets(1));
    var output1 = DebeziumEngineManager.debeziumComponent().collect(input1);
    assertNoProgress(input1, output1);
    // An annoying characteristic of the debezium engine makes it such that the
    // HAS_WAITED_LONG_ENOUGH_FOR_INITIAL_RECORD reason is never triggered in cases
    // where the (logical) database WAL is consumed starting from the last offset
    // and the WAL is not making any logical progress.
    assertCompletionReason(output1, DebeziumComponent.Output.CompletionReason.HAS_COLLECTED_LONG_ENOUGH);
  }

  @Test
  public void testHeartbeatsProgress() {
    var state0 = state();
    // Insert just one record.
    // We need this or Debezium will never actually start firing heartbeats.
    testdb.with(INITIAL_INSERT.getFirst().sql()).with("CHECKPOINT");
    // Consume the WAL from right before the insert to right after it.
    var input1 = new DebeziumComponent.Input(config(), state0, completionTargets(10));
    // Make sure there's more entries in the WAL after the insert.
    testdb.with("SELECT GENERATE_SERIES(1, 10000) INTO s").with("CHECKPOINT");
    var output1 = DebeziumEngineManager.debeziumComponent().collect(input1);
    assertCompletionReason(output1, DebeziumComponent.Output.CompletionReason.HAS_EVENTS_OUT_OF_BOUNDS);
  }

  @Test
  public void testCRUD() {
    final var maxRecords = 20;
    var state0 = state();

    INITIAL_INSERT.stream().map(Change::sql).forEach(testdb::with);
    testdb.with("CHECKPOINT");
    var input1 = new DebeziumComponent.Input(config(), state0, completionTargets(maxRecords));
    var output1 = DebeziumEngineManager.debeziumComponent().collect(input1);
    assertProgress(input1, output1);
    assertData(output1, INITIAL_INSERT);
    assertCompletionReason(output1, DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD);

    DELETE_EVENTLOG.stream().map(Change::sql).forEach(testdb::with);
    testdb.with("CHECKPOINT");
    var input2 = new DebeziumComponent.Input(config(), output1.state(), completionTargets(maxRecords));
    var output2 = DebeziumEngineManager.debeziumComponent().collect(input2);
    assertProgress(input2, output2);
    assertData(output2, DELETE_EVENTLOG);
    assertCompletionReason(output2, DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD);

    UPDATE_KV.stream().map(Change::sql).forEach(testdb::with);
    testdb.with("CHECKPOINT");
    var input3 = new DebeziumComponent.Input(config(), output2.state(), completionTargets(maxRecords));
    var output3 = DebeziumEngineManager.debeziumComponent().collect(input3);
    assertProgress(input3, output3);
    assertData(output3, UPDATE_KV);
    assertCompletionReason(output3, DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD);

    SUBSEQUENT_INSERT.stream().map(Change::sql).forEach(testdb::with);
    testdb.with("CHECKPOINT");
    var input4 = new DebeziumComponent.Input(config(), output3.state(), completionTargets(maxRecords));
    var output4 = DebeziumEngineManager.debeziumComponent().collect(input4);
    assertProgress(input4, output4);
    assertData(output4, SUBSEQUENT_INSERT);
    assertCompletionReason(output4, DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD);

    var input24 = new DebeziumComponent.Input(config(), output1.state(), input4.completionTargets());
    var output24 = DebeziumEngineManager.debeziumComponent().collect(input24);
    assertProgress(input24, output24);
    assertData(output24, DELETE_EVENTLOG, UPDATE_KV, SUBSEQUENT_INSERT);
    assertCompletionReason(output24, DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD);

    var input14 = new DebeziumComponent.Input(config(), state0, input4.completionTargets());
    var output14 = DebeziumEngineManager.debeziumComponent().collect(input14);
    assertData(output14, INITIAL_INSERT, DELETE_EVENTLOG, UPDATE_KV, SUBSEQUENT_INSERT);
    assertCompletionReason(output14, DebeziumComponent.Output.CompletionReason.HAS_WAITED_LONG_ENOUGH_FOR_SUBSEQUENT_RECORD);
  }

  @Test
  public void testCompletesWithEnoughRecords() {
    final int numRows = 10_000;
    final int maxRecords = 10;
    var state0 = state();

    testdb
        .with("INSERT INTO kv (v) SELECT n::VARCHAR FROM GENERATE_SERIES(1, %d) AS n", numRows)
        .with("CHECKPOINT");
    var input1 = new DebeziumComponent.Input(config(), state0, completionTargets(maxRecords));
    var output1 = DebeziumEngineManager.debeziumComponent().collect(input1);
    assertProgress(input1, output1);
    assertCompletionReason(output1, DebeziumComponent.Output.CompletionReason.HAS_COLLECTED_ENOUGH_RECORDS);
    // We actually get more than we bargained for, but that's OK.
    assertDataCountWithinBounds(maxRecords + 1, output1, numRows);

    var input2 = new DebeziumComponent.Input(config(), state0, completionTargets(numRows));
    var output2 = DebeziumEngineManager.debeziumComponent().collect(input2);
    assertProgress(input2, output2);
    assertCompletionReason(output2, DebeziumComponent.Output.CompletionReason.HAS_COLLECTED_ENOUGH_RECORDS);
    Assertions.assertEquals(numRows, output2.data().size());
  }

  @Test
  public void testCompletesWhenOutOfBounds() {
    final int numRowsInBatch = 10_000;
    final int maxRecords = 100_000;
    var state0 = state();
    testdb
        .with("INSERT INTO kv (v) SELECT n::VARCHAR FROM GENERATE_SERIES(1, %d) AS n", numRowsInBatch)
        .with("CHECKPOINT");
    var completionTargets1 = completionTargets(maxRecords);
    testdb
        .with("INSERT INTO kv (v) SELECT n::VARCHAR FROM GENERATE_SERIES(1, %d) AS n", numRowsInBatch)
        .with("CHECKPOINT");
    var input1 = new DebeziumComponent.Input(config(), state0, completionTargets1);
    var output1 = DebeziumEngineManager.debeziumComponent().collect(input1);
    assertProgress(input1, output1);
    assertCompletionReason(output1, DebeziumComponent.Output.CompletionReason.HAS_EVENTS_OUT_OF_BOUNDS);
    assertDataCountWithinBounds(numRowsInBatch, output1, 2 * numRowsInBatch);
  }

  static void assertNoProgress(DebeziumComponent.Input input, DebeziumComponent.Output output) {
    Assertions.assertTrue(input.state().schema().isEmpty());
    Assertions.assertTrue(output.state().schema().isEmpty());
    Assertions.assertTrue(output.data().isEmpty());
    Assertions.assertEquals(
        Jsons.serialize(Jsons.jsonNode(input.state().offset().debeziumOffset())),
        Jsons.serialize(Jsons.jsonNode(output.state().offset().debeziumOffset())));
    Assertions.assertEquals(0, output.executionSummary().records().count());
    Assertions.assertNotEquals(Set.of(), output.completionReasons());
  }

  static void assertProgress(DebeziumComponent.Input input, DebeziumComponent.Output output) {
    Assertions.assertTrue(input.state().schema().isEmpty());
    Assertions.assertTrue(output.state().schema().isEmpty());
    Assertions.assertNotEquals(
        Jsons.serialize(Jsons.jsonNode(input.state().offset().debeziumOffset())),
        Jsons.serialize(Jsons.jsonNode(output.state().offset().debeziumOffset())));
    Assertions.assertNotEquals(0, output.executionSummary().events().count());
    Assertions.assertNotEquals(0, output.executionSummary().records().count());
    Assertions.assertNotEquals(Set.of(), output.completionReasons());
  }

  @SafeVarargs
  @SuppressWarnings("varargs")
  static void assertData(DebeziumComponent.Output output, List<Change>... expected) {
    var expectedAsInsertsOrDeletes = Stream.of(expected)
        .flatMap(List::stream)
        .map(c -> c.kind() == Change.Kind.UPDATE ? Change.insert(c.table(), c.newValue()) : c)
        .toList();
    var actualAsInsertsOrDeletes = output.data().stream()
        .map(r -> {
          var table = Change.Table.valueOf(r.source().get("table").asText().toUpperCase());
          var before = r.before().isNull() ? null : Change.Value.valueOf(r.before().get(table.valueColumnName).asText().toUpperCase());
          var after = r.after().isNull() ? null : Change.Value.valueOf(r.after().get(table.valueColumnName).asText().toUpperCase());
          return after == null ? Change.delete(table, before) : Change.insert(table, after);
        })
        .toList();
    Assertions.assertEquals(expectedAsInsertsOrDeletes, actualAsInsertsOrDeletes);
  }

  static void assertCompletionReason(DebeziumComponent.Output output, DebeziumComponent.Output.CompletionReason expected) {
    Assertions.assertNotEquals(Set.of(), output.completionReasons());
    Assertions.assertTrue(output.completionReasons().contains(expected),
        String.format("%s not found in %s", expected, output.completionReasons()));
  }

  static void assertDataCountWithinBounds(int lowerBoundInclusive, DebeziumComponent.Output output, int upperBoundExclusive) {
    Assertions.assertTrue(output.data().size() >= lowerBoundInclusive,
        String.format("expected no less than %d records, obtained %d", lowerBoundInclusive, output.data().size()));
    Assertions.assertTrue(output.data().size() < upperBoundExclusive,
        String.format("expected less than %d records, obtained %d", upperBoundExclusive, output.data().size()));
  }

  DebeziumComponent.Input.Config config() {
    return new RelationalConfigBuilder<>()
        .withDatabaseHost(testdb.getContainer().getHost())
        .withDatabasePort(testdb.getContainer().getFirstMappedPort())
        .withDatabaseUser(testdb.getUserName())
        .withDatabasePassword(testdb.getPassword())
        .withDatabaseName(testdb.getDatabaseName())
        .withCatalog(configuredCatalog())
        .withConnector(PostgresConnector.class)
        .withHeartbeats(Duration.ofMillis(100))
        .with("plugin.name", "pgoutput")
        .with("snapshot.mode", "initial")
        .with("slot.name", testdb.getReplicationSlotName())
        .with("publication.name", testdb.getPublicationName())
        .with("publication.autocreate.mode", "disabled")
        .with("flush.lsn.source", "false")
        .build();
  }

  DebeziumComponent.State state() {
    try {
      final PgLsn lsn = PgLsn.fromPgString(testdb.getDatabase().query(ctx -> ctx
          .selectFrom("pg_current_wal_lsn()")
          .fetchSingle(0, String.class)));
      long txid = testdb.getDatabase().query(ctx -> ctx
          .selectFrom("txid_current()")
          .fetchSingle(0, Long.class));
      var now = Instant.now();
      var value = new HashMap<String, Object>();
      value.put("transaction_id", null);
      value.put("lsn", lsn.asLong());
      value.put("txId", txid);
      value.put("ts_usec", now.toEpochMilli() * 1_000);
      var valueJson = Jsons.jsonNode(value);
      var keyJson = Jsons.arrayNode()
          .add(testdb.getDatabaseName())
          .add(Jsons.jsonNode(Map.of("server", testdb.getDatabaseName())));
      var offset = new DebeziumComponent.State.Offset(Map.of(keyJson, valueJson));
      return new DebeziumComponent.State(offset, Optional.empty());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  DebeziumComponent.Input.CompletionTargets completionTargets(long maxRecords) {
    try {
      final PgLsn lsn = PgLsn.fromPgString(testdb.getDatabase().query(ctx -> ctx
          .selectFrom("pg_current_wal_insert_lsn()")
          .fetchSingle(0, String.class)));
      return new DebeziumComponent.Input.CompletionTargets(
          r -> isWithinBounds(lsn, r),
          maxRecords,
          Duration.ofSeconds(5),
          Duration.ofSeconds(1),
          Duration.ofSeconds(1),
          0.0);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  static boolean isWithinBounds(PgLsn upperBound, DebeziumComponent.Output.Record record) {
    PgLsn eventLsn;
    var sourceOffset = record.debeziumSourceRecord().map(SourceRecord::sourceOffset).orElse(Map.of());
    if (sourceOffset.containsKey("lsn")) {
      eventLsn = PgLsn.fromLong((Long) sourceOffset.get("lsn"));
    } else if (record.source().has("lsn")) {
      eventLsn = PgLsn.fromLong(record.source().get("lsn").asLong());
    } else {
      return true;
    }
    return eventLsn.compareTo(upperBound) <= 0;
  }

}
