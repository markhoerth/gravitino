/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.elasticsearch.client.ResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read path operations for an Elasticsearch catalog: indices are exposed as tables, with columns
 * derived from the index mapping.
 *
 * <p>Elasticsearch has no level between a cluster and an index, so a single schema named {@code
 * default} stands in for one. Inventing a schema per index prefix would encode a naming convention
 * the cluster does not actually have.
 *
 * <p>Every write operation is unsupported. Creating an index through Gravitino means writing a
 * mapping, which fixes the shape of every document written afterwards by every other application.
 * That decision belongs in Elasticsearch, not in a catalog.
 *
 * <p>Note this implements {@link org.apache.gravitino.connector.SupportsSchemas}, taking {@link
 * NameIdentifier} and {@link Namespace}. The similarly named {@code org.apache.gravitino
 * .SupportsSchemas} takes strings and compiles just as cleanly, but {@code CatalogManager} checks
 * for this one, and the wrong choice fails at runtime with "Catalog does not support schema
 * operations".
 */
public class ElasticsearchCatalogOperations
    implements CatalogOperations, SupportsSchemas, TableCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchCatalogOperations.class);

  /** The one schema this catalog exposes, standing in for a level Elasticsearch does not have. */
  public static final String DEFAULT_SCHEMA = "default";

  private static final String WRITE_UNSUPPORTED =
      "The Gravitino Elasticsearch catalog is read only. Indices and mappings must be managed "
          + "directly in Elasticsearch.";

  private static final String CAT_INDEX = "index";
  private static final String CAT_STATUS = "status";
  private static final String CAT_HEALTH = "health";
  private static final String CAT_DOCS_COUNT = "docs.count";
  private static final String CAT_STORE_SIZE = "store.size";
  private static final String STATUS_CLOSE = "close";

  private ElasticsearchClient client;
  private boolean includeSystemIndices;

  /**
   * Rows from the most recent listing, so that loadTable can report docs count and store size
   * without a third round trip. A direct loadTable on an index never listed simply omits them.
   */
  private final Map<String, Map<String, String>> catStats = new ConcurrentHashMap<>();

  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    ElasticsearchCatalogPropertiesMetadata.validate(conf);

    this.includeSystemIndices =
        ElasticsearchCatalogPropertiesMetadata.booleanOrDefault(
            conf, ElasticsearchCatalogPropertiesMetadata.INCLUDE_SYSTEM_INDICES, false);
    this.client = new ElasticsearchClient(conf);

    LOG.info(
        "Initialized Elasticsearch catalog {} against {}",
        info == null ? "unknown" : info.name(),
        ElasticsearchCatalogPropertiesMetadata.hosts(conf));
  }

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {
    ElasticsearchCatalogPropertiesMetadata.validate(properties);

    try (ElasticsearchClient probe = new ElasticsearchClient(properties)) {
      JsonNode root = probe.ping();
      LOG.info(
          "Reached Elasticsearch cluster {} running version {}",
          text(root, "cluster_name"),
          version(root));
    } catch (IOException e) {
      throw new IOException(
          "Failed to reach Elasticsearch at "
              + ElasticsearchCatalogPropertiesMetadata.hosts(properties)
              + ": "
              + e.getMessage(),
          e);
    }
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    return new NameIdentifier[] {NameIdentifier.of(namespace, DEFAULT_SCHEMA)};
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    String schemaName = ident.name();
    if (!DEFAULT_SCHEMA.equals(schemaName)) {
      throw new NoSuchSchemaException(
          "Elasticsearch catalog exposes only the %s schema, not %s", DEFAULT_SCHEMA, schemaName);
    }

    return ElasticsearchSchema.builder()
        .withName(DEFAULT_SCHEMA)
        .withComment("All indices in the Elasticsearch cluster")
        .withProperties(ImmutableMap.of())
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties) {
    throw new UnsupportedOperationException(WRITE_UNSUPPORTED);
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes) {
    throw new UnsupportedOperationException(WRITE_UNSUPPORTED);
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    throw new UnsupportedOperationException(WRITE_UNSUPPORTED);
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    requireDefaultSchema(NameIdentifier.of(namespace.levels()).name());

    List<JsonNode> rows;
    try {
      rows = client.catIndices();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to list Elasticsearch indices", e);
    }

    List<NameIdentifier> tables = Lists.newArrayListWithExpectedSize(rows.size());
    for (JsonNode row : rows) {
      String index = text(row, CAT_INDEX);
      if (index == null || isFilteredOut(index, text(row, CAT_STATUS))) {
        continue;
      }

      catStats.put(index, toCatStats(row));
      tables.add(NameIdentifier.of(namespace, index));
    }

    tables.sort(Comparator.comparing(NameIdentifier::name));
    return tables.toArray(new NameIdentifier[0]);
  }

  @Override
  public Table loadTable(NameIdentifier tableIdent) throws NoSuchTableException {
    requireDefaultSchema(NameIdentifier.of(tableIdent.namespace().levels()).name());
    String index = tableIdent.name();

    JsonNode mappings;
    JsonNode settings;
    try {
      mappings = client.getMapping(index);
      settings = client.getSettings(index);
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == 404) {
        throw new NoSuchTableException(e, "Elasticsearch index %s does not exist", index);
      }
      throw new UncheckedIOException("Failed to read Elasticsearch index " + index, e);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read Elasticsearch index " + index, e);
    }

    List<Column> columns = ElasticsearchMappingParser.parseColumns(mappings);

    return ElasticsearchTable.builder()
        .withName(index)
        .withComment("Elasticsearch index " + index)
        .withColumns(columns.toArray(new Column[0]))
        .withProperties(indexProperties(index, settings))
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes) {
    throw new UnsupportedOperationException(WRITE_UNSUPPORTED);
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes) {
    throw new UnsupportedOperationException(WRITE_UNSUPPORTED);
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    throw new UnsupportedOperationException(WRITE_UNSUPPORTED);
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    throw new UnsupportedOperationException(WRITE_UNSUPPORTED);
  }

  @Override
  public void close() throws IOException {
    if (client != null) {
      client.close();
      client = null;
    }
    catStats.clear();
  }

  /**
   * Closed indices cannot be mapped or read, and system indices are Elasticsearch's own bookkeeping
   * rather than anything a user modeled.
   */
  private boolean isFilteredOut(String index, @Nullable String status) {
    if (STATUS_CLOSE.equals(status)) {
      return true;
    }
    return index.startsWith(".") && !includeSystemIndices;
  }

  private void requireDefaultSchema(String schemaName) {
    if (!DEFAULT_SCHEMA.equals(schemaName)) {
      throw new NoSuchSchemaException(
          "Elasticsearch catalog exposes only the %s schema, not %s", DEFAULT_SCHEMA, schemaName);
    }
  }

  /**
   * Builds table properties from the index settings, adding the listing columns when this index was
   * seen through a recent listTables. A direct loadTable omits them rather than paying for a third
   * call to report a document count nobody asked for.
   */
  private Map<String, String> indexProperties(String index, JsonNode settings) {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

    put(properties, ElasticsearchTablePropertiesMetadata.INDEX_UUID, text(settings, "uuid"));
    put(
        properties,
        ElasticsearchTablePropertiesMetadata.NUMBER_OF_SHARDS,
        text(settings, "number_of_shards"));
    put(
        properties,
        ElasticsearchTablePropertiesMetadata.NUMBER_OF_REPLICAS,
        text(settings, "number_of_replicas"));
    put(
        properties,
        ElasticsearchTablePropertiesMetadata.CREATION_DATE,
        text(settings, "creation_date"));

    Map<String, String> stats = catStats.get(index);
    if (stats != null) {
      properties.putAll(stats);
    }

    return properties.build();
  }

  private static Map<String, String> toCatStats(JsonNode row) {
    ImmutableMap.Builder<String, String> stats = ImmutableMap.builder();
    put(stats, ElasticsearchTablePropertiesMetadata.STATUS, text(row, CAT_STATUS));
    put(stats, ElasticsearchTablePropertiesMetadata.HEALTH, text(row, CAT_HEALTH));
    put(stats, ElasticsearchTablePropertiesMetadata.DOCS_COUNT, text(row, CAT_DOCS_COUNT));
    put(stats, ElasticsearchTablePropertiesMetadata.STORE_SIZE, text(row, CAT_STORE_SIZE));
    return stats.build();
  }

  private static void put(
      ImmutableMap.Builder<String, String> properties, String key, @Nullable String value) {
    if (value != null) {
      properties.put(key, value);
    }
  }

  @Nullable
  private static String version(JsonNode root) {
    JsonNode version = root == null ? null : root.get("version");
    return text(version, "number");
  }

  @Nullable
  private static String text(@Nullable JsonNode node, String field) {
    if (node == null) {
      return null;
    }
    JsonNode value = node.get(field);
    return value == null || value.isNull() ? null : value.asText();
  }
}
