/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.components.debezium;

import io.debezium.spi.common.ReplacementFunction;
import io.debezium.storage.file.history.FileSchemaHistory;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;

public class ConfigBuilder<B extends ConfigBuilder<B>> {

  protected Properties props;

  public ConfigBuilder() {
    props = new Properties();

    // default values from debezium CommonConnectorConfig
    props.setProperty("max.batch.size", "2048");
    props.setProperty("max.queue.size", "8192");

    // Disabling retries because debezium startup time might exceed our 60-second wait limit
    // The maximum number of retries on connection errors before failing (-1 = no limit, 0 = disabled, >
    // 0 = num of retries).
    props.setProperty("errors.max.retries", "0");

    // This property must be strictly less than errors.retry.delay.max.ms
    // (https://github.com/debezium/debezium/blob/bcc7d49519a4f07d123c616cfa45cd6268def0b9/debezium-core/src/main/java/io/debezium/util/DelayStrategy.java#L135)
    props.setProperty("errors.retry.delay.initial.ms", "299");
    props.setProperty("errors.retry.delay.max.ms", "300");

    // https://debezium.io/documentation/reference/2.2/configuration/avro.html
    props.setProperty("key.converter.schemas.enable", "false");
    props.setProperty("value.converter.schemas.enable", "false");

    // By default "decimal.handing.mode=precise" which's caused returning this value as a binary.
    // The "double" type may cause a loss of precision, so set Debezium's config to store it as a String
    // explicitly in its Kafka messages for more details see:
    // https://debezium.io/documentation/reference/2.2/connectors/postgresql.html#postgresql-decimal-types
    // https://debezium.io/documentation/faq/#how_to_retrieve_decimal_field_from_binary_representation
    props.setProperty("decimal.handling.mode", "string");

    props.setProperty("offset.storage", FileOffsetBackingStore.class.getCanonicalName());
    props.setProperty("offset.flush.interval.ms", "1000"); // todo: make this longer

    props.setProperty("debezium.embedded.shutdown.pause.before.interrupt.ms", "1000");
  }

  public DebeziumComponent.Input.Config build() {
    return new DebeziumComponent.Input.Config((Properties) props.clone());
  }

  @SuppressWarnings("unchecked")
  protected final B self() {
    return (B) this;
  }

  public B withSchemaHistory() {
    props.setProperty("schema.history.internal", FileSchemaHistory.class.getCanonicalName());
    props.setProperty("schema.history.internal.store.only.captured.databases.ddl", "true");
    return self();
  }

  public B withDebeziumName(String name) {
    props.setProperty("name", name);
    // WARNING:
    // Never change the value of this otherwise all the connectors would start syncing from scratch.
    props.setProperty("topic.prefix", sanitizeTopicPrefix(name));
    return self();
  }

  public B withConnector(Class<? extends Connector> connectorClass) {
    props.setProperty("connector.class", connectorClass.getCanonicalName());
    return self();
  }

  public B withHeartbeats(Duration heartbeatInterval) {
    props.setProperty("heartbeat.interval.ms", Long.toString(heartbeatInterval.toMillis()));
    return self();
  }

  public B with(String key, String value) {
    props.setProperty(key, value);
    return self();
  }

  public B with(Map<?, ?> properties) {
    props.putAll(properties);
    return self();
  }

  public static String sanitizeTopicPrefix(final String topicName) {
    StringBuilder sanitizedNameBuilder = new StringBuilder(topicName.length());
    boolean changed = false;

    for (int i = 0; i < topicName.length(); ++i) {
      char c = topicName.charAt(i);
      if (isValidCharacter(c)) {
        sanitizedNameBuilder.append(c);
      } else {
        sanitizedNameBuilder.append(ReplacementFunction.UNDERSCORE_REPLACEMENT.replace(c));
        changed = true;
      }
    }

    if (changed) {
      return sanitizedNameBuilder.toString();
    } else {
      return topicName;
    }
  }

  // We need to keep the validation rule the same as debezium engine, which is defined here:
  // https://github.com/debezium/debezium/blob/c51ef3099a688efb41204702d3aa6d4722bb4825/debezium-core/src/main/java/io/debezium/schema/AbstractTopicNamingStrategy.java#L178
  private static boolean isValidCharacter(char c) {
    return c == '.' || c == '_' || c == '-' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9';
  }

}
