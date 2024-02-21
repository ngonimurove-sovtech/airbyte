/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.components.debezium;

import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.v0.SyncMode;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.codehaus.plexus.util.StringUtils;

public class RelationalConfigBuilder<B extends RelationalConfigBuilder<B>> extends ConfigBuilder<B> {

  public RelationalConfigBuilder() {
    super();

    // https://debezium.io/documentation/reference/2.2/connectors/postgresql.html#postgresql-property-max-queue-size-in-bytes
    props.setProperty("max.queue.size.in.bytes", Long.toString(256L * 1024 * 1024));
  }

  public B withDatabaseHost(String host) {
    return with("database.hostname", host);
  }

  public B withDatabasePort(int port) {
    return with("database.port", Integer.toString(port));
  }

  public B withDatabaseUser(String user) {
    return with("database.user", user);
  }

  public B withDatabasePassword(String password) {
    return with("database.password", password);
  }

  public B withDatabaseName(String name) {
    return with("database.dbname", name)
        .withDebeziumName(name);
  }

  public B withCatalog(ConfiguredAirbyteCatalog catalog) {
    return with("table.include.list", getTableIncludelist(catalog))
        .with("column.include.list", getColumnIncludeList(catalog));
  }

  public static String getTableIncludelist(final ConfiguredAirbyteCatalog catalog) {
    // Turn "stream": {
    // "namespace": "schema1"
    // "name": "table1
    // },
    // "stream": {
    // "namespace": "schema2"
    // "name": "table2
    // } -------> info "schema1.table1, schema2.table2"

    return catalog.getStreams().stream()
        .filter(s -> s.getSyncMode() == SyncMode.INCREMENTAL)
        .map(ConfiguredAirbyteStream::getStream)
        .map(stream -> stream.getNamespace() + "." + stream.getName())
        // debezium needs commas escaped to split properly
        .map(x -> StringUtils.escape(Pattern.quote(x), ",".toCharArray(), "\\,"))
        .collect(Collectors.joining(","));
  }

  public static String getColumnIncludeList(final ConfiguredAirbyteCatalog catalog) {
    // Turn "stream": {
    // "namespace": "schema1"
    // "name": "table1"
    // "jsonSchema": {
    // "properties": {
    // "column1": {
    // },
    // "column2": {
    // }
    // }
    // }
    // } -------> info "schema1.table1.(column1 | column2)"

    return catalog.getStreams().stream()
        .filter(s -> s.getSyncMode() == SyncMode.INCREMENTAL)
        .map(ConfiguredAirbyteStream::getStream)
        .map(s -> {
          final String fields = parseFields(s.getJsonSchema().get("properties").fieldNames());
          // schema.table.(col1|col2)
          return Pattern.quote(s.getNamespace() + "." + s.getName()) + (StringUtils.isNotBlank(fields) ? "\\." + fields : "");
        })
        .map(x -> StringUtils.escape(x, ",".toCharArray(), "\\,"))
        .collect(Collectors.joining(","));
  }

  private static String parseFields(final Iterator<String> fieldNames) {
    if (fieldNames == null || !fieldNames.hasNext()) {
      return "";
    }
    final Iterable<String> iter = () -> fieldNames;
    return StreamSupport.stream(iter.spliterator(), false)
        .map(Pattern::quote)
        .collect(Collectors.joining("|", "(", ")"));
  }

}
