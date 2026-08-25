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
package org.apache.gravitino.catalog.elasticsearch.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.elasticsearch.ElasticsearchCatalogOperations;
import org.apache.gravitino.catalog.elasticsearch.ElasticsearchCatalogPropertiesMetadata;
import org.apache.gravitino.catalog.elasticsearch.ElasticsearchTypeConverter;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * End to end coverage of the read path against a real Elasticsearch cluster.
 *
 * <p>The unit tests parse hand written mappings, which proves the converter's rules but not that
 * Elasticsearch writes mappings in the shape those fixtures assume. Here an index is created
 * through the cluster's own mapping API and read back through {@link
 * ElasticsearchCatalogOperations}, so the assertions run against whatever the cluster actually
 * stores and returns.
 *
 * <p>The fixture index carries one field per branch the converter handles: every scalar family, a
 * dense vector, both object shapes, a nested object, an explicit struct and an alias.
 */
@EnabledIf("dockerAvailable")
public class CatalogElasticsearchIT {

  private static final String IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:8.15.0";

  private static final int ES_PORT = 9200;

  private static final String INDEX = "fixture_all";

  private static final Namespace NAMESPACE = Namespace.of("metalake", "catalog", "default");

  /** Every mapping branch the converter handles, in one index. */
  private static final String MAPPING =
      "{\"mappings\":{\"properties\":{"
          + "\"id\":{\"type\":\"keyword\",\"meta\":{\"description\":\"primary id\"}},"
          + "\"title\":{\"type\":\"text\"},"
          + "\"created_at\":{\"type\":\"date\"},"
          + "\"score\":{\"type\":\"double\"},"
          + "\"visits\":{\"type\":\"long\"},"
          + "\"active\":{\"type\":\"boolean\"},"
          + "\"ip_addr\":{\"type\":\"ip\"},"
          + "\"blob\":{\"type\":\"binary\"},"
          + "\"loc\":{\"type\":\"geo_point\"},"
          + "\"ranges\":{\"type\":\"integer_range\"},"
          + "\"embedding\":{\"type\":\"dense_vector\",\"dims\":384,\"index\":true,"
          + "\"similarity\":\"cosine\"},"
          + "\"bare_object\":{\"type\":\"object\"},"
          + "\"empty_props\":{\"type\":\"object\",\"properties\":{}},"
          + "\"nested_items\":{\"type\":\"nested\",\"properties\":"
          + "{\"sku\":{\"type\":\"keyword\"},\"qty\":{\"type\":\"integer\"}}},"
          + "\"addr\":{\"type\":\"object\",\"properties\":"
          + "{\"city\":{\"type\":\"keyword\"},\"zip\":{\"type\":\"keyword\"}}},"
          + "\"title_alias\":{\"type\":\"alias\",\"path\":\"title\"},"
          + "\"documented\":{\"type\":\"keyword\","
          + "\"meta\":{\"description\":\"customer identifier\"}},"
          + "\"documented_alt\":{\"type\":\"keyword\","
          + "\"meta\":{\"comment\":\"fallback key\"}},"
          + "\"documented_vector\":{\"type\":\"dense_vector\",\"dims\":128,"
          + "\"index\":true,\"similarity\":\"dot_product\","
          + "\"meta\":{\"description\":\"embedding of the title\"}}"
          + "}}}";

  private static final Set<String> EXPECTED_COLUMNS =
      new TreeSet<>(
          Arrays.asList(
              "id",
              "title",
              "created_at",
              "score",
              "visits",
              "active",
              "ip_addr",
              "blob",
              "loc",
              "ranges",
              "embedding",
              "bare_object",
              "empty_props",
              "nested_items",
              "addr",
              "title_alias",
              "documented",
              "documented_alt",
              "documented_vector"));

  private static GenericContainer<?> container;

  private static ElasticsearchCatalogOperations operations;

  /**
   * Guards the whole class, so a machine without a reachable Docker daemon skips rather than fails.
   *
   * @return true if testcontainers can reach a Docker daemon
   */
  static boolean dockerAvailable() {
    try {
      return DockerClientFactory.instance().isDockerAvailable();
    } catch (Throwable t) {
      return false;
    }
  }

  /**
   * Starts a single node cluster, creates the fixture index through it, and points the catalog at
   * the mapped port.
   *
   * @throws Exception if the container does not start or the index cannot be created
   */
  @BeforeAll
  public static void startCluster() throws Exception {
    container =
        new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withExposedPorts(ES_PORT)
            .withEnv("discovery.type", "single-node")
            .withEnv("xpack.security.enabled", "false")
            .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
            .waitingFor(
                Wait.forHttp("/").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(5)));
    container.start();

    String uri = "http://" + container.getHost() + ":" + container.getMappedPort(ES_PORT);
    createFixtureIndex(uri);

    operations = new ElasticsearchCatalogOperations();
    operations.initialize(
        ImmutableMap.of(ElasticsearchCatalogPropertiesMetadata.HOSTS, uri), null, null);
  }

  /**
   * Closes the catalog and stops the cluster.
   *
   * @throws Exception if the catalog cannot be closed
   */
  @AfterAll
  public static void stopCluster() throws Exception {
    if (operations != null) {
      operations.close();
      operations = null;
    }
    if (container != null) {
      container.stop();
      container = null;
    }
  }

  /** The fixture index is listed as a table. */
  @Test
  public void testListTables() {
    Set<String> tables =
        Arrays.stream(operations.listTables(NAMESPACE))
            .map(NameIdentifier::name)
            .collect(Collectors.toCollection(TreeSet::new));

    assertTrue(tables.contains(INDEX), "listTables did not report " + INDEX + ", got " + tables);
  }

  /** Every mapped field becomes a column, and nothing else does. */
  @Test
  public void testLoadTableColumnSet() {
    Set<String> actual = new TreeSet<>(columns().keySet());
    assertEquals(EXPECTED_COLUMNS, actual);
  }

  /**
   * Each column carries the type {@link ElasticsearchTypeConverter} declares for its mapping type,
   * so the expectations are read from the converter's own table rather than restated here.
   */
  @Test
  public void testColumnTypes() {
    Map<String, Column> columns = columns();

    assertEquals(scalar("keyword"), columns.get("id").dataType());
    assertEquals(scalar("text"), columns.get("title").dataType());
    assertEquals(scalar("date"), columns.get("created_at").dataType());
    assertEquals(scalar("double"), columns.get("score").dataType());
    assertEquals(scalar("long"), columns.get("visits").dataType());
    assertEquals(scalar("boolean"), columns.get("active").dataType());
    assertEquals(scalar("ip"), columns.get("ip_addr").dataType());
    assertEquals(scalar("binary"), columns.get("blob").dataType());
    assertEquals(scalar("geo_point"), columns.get("loc").dataType());
    assertEquals(scalar("integer_range"), columns.get("ranges").dataType());
    assertEquals(
        scalar(ElasticsearchTypeConverter.DENSE_VECTOR), columns.get("embedding").dataType());

    assertEquals(
        Types.ExternalType.of(ElasticsearchTypeConverter.OBJECT),
        columns.get("bare_object").dataType());
    assertEquals(
        Types.ExternalType.of(ElasticsearchTypeConverter.OBJECT),
        columns.get("empty_props").dataType());

    assertEquals(
        Types.StructType.of(
            Types.StructType.Field.nullableField("city", scalar("keyword"), null),
            Types.StructType.Field.nullableField("zip", scalar("keyword"), null)),
        columns.get("addr").dataType());

    // The cluster returns a node's properties sorted by field name, not in the order they were
    // declared, so the struct fields are qty then sku rather than sku then qty.
    assertEquals(
        Types.ListType.of(
            Types.StructType.of(
                Types.StructType.Field.nullableField("qty", scalar("integer"), null),
                Types.StructType.Field.nullableField("sku", scalar("keyword"), null)),
            true),
        columns.get("nested_items").dataType());

    assertEquals(scalar("text"), columns.get("title_alias").dataType());
  }

  /**
   * An object with no usable shape is an external type, not an empty struct. The two spellings are
   * written differently but the cluster normalizes both to a bare {@code {"type": "object"}}, so
   * the parser never sees the empty {@code properties} the fixture declared.
   */
  @Test
  public void testObjectWithoutProperties() {
    Map<String, Column> columns = columns();
    Type expected = Types.ExternalType.of(ElasticsearchTypeConverter.OBJECT);

    assertEquals(expected, columns.get("bare_object").dataType());
    assertEquals(expected, columns.get("empty_props").dataType());
  }

  /** An alias reports the type of the field it points at. */
  @Test
  public void testAliasResolution() {
    Map<String, Column> columns = columns();
    assertEquals(columns.get("title").dataType(), columns.get("title_alias").dataType());
  }

  /**
   * A field with no {@code meta} block keeps the comment synthesized from its mapping. {@code id}
   * cannot stand for this case: the fixture has always given it a {@code meta.description}, which
   * is exactly what the parser now reports, so an undescribed field carries the fallback instead.
   */
  @Test
  public void testUndocumentedColumnComment() {
    Column title = columns().get("title");
    assertNotNull(title.comment(), "title carries no comment");
    assertEquals("elasticsearch type: text", title.comment());

    assertEquals(
        "elasticsearch type: text; alias for: title", columns().get("title_alias").comment());
  }

  /** The description the fixture has always carried on {@code id} now reaches the column. */
  @Test
  public void testDescribedColumnCommentOnId() {
    assertEquals("primary id", columns().get("id").comment());
  }

  /** {@code meta.description} becomes the comment outright, with no mapping type left in it. */
  @Test
  public void testColumnCommentFromDescription() {
    String comment = columns().get("documented").comment();

    assertEquals("customer identifier", comment);
    assertFalse(comment.contains("keyword"), "the mapping type leaked into " + comment);
    assertFalse(comment.contains("elasticsearch type"), "the synthesized prefix survived");
  }

  /** {@code meta.comment} is read when the field carries no {@code meta.description}. */
  @Test
  public void testColumnCommentFromCommentKey() {
    assertEquals("fallback key", columns().get("documented_alt").comment());
  }

  /**
   * A described vector keeps the attributes its Gravitino type cannot carry: {@code list<float>}
   * says nothing about how long the vector is or how it is compared.
   */
  @Test
  public void testColumnCommentKeepsVectorAttributes() {
    String comment = columns().get("documented_vector").comment();

    assertTrue(
        comment.startsWith("embedding of the title ("), "unexpected comment shape: " + comment);
    assertTrue(comment.contains("128"), "dims missing from " + comment);
    assertTrue(comment.contains("dot_product"), "similarity missing from " + comment);
  }

  /** Loading an index that does not exist is a missing table, not a transport failure. */
  @Test
  public void testNonexistentTable() {
    NameIdentifier missing = NameIdentifier.of(NAMESPACE, "no_such_index");
    assertThrows(NoSuchTableException.class, () -> operations.loadTable(missing));
  }

  /** The fixture index's columns, keyed by name and kept in mapping order. */
  private static Map<String, Column> columns() {
    Table table = operations.loadTable(NameIdentifier.of(NAMESPACE, INDEX));

    Map<String, Column> columns = new LinkedHashMap<>();
    for (Column column : table.columns()) {
      columns.put(column.name(), column);
    }
    return columns;
  }

  /** The Gravitino type the converter declares for a non structural mapping type. */
  private static Type scalar(String esType) {
    return ElasticsearchTypeConverter.fromMappingType(esType);
  }

  /**
   * Creates the fixture index with the low level REST client the connector itself uses, so the test
   * adds no HTTP dependency of its own.
   */
  private static void createFixtureIndex(String uri) throws Exception {
    try (RestClient client = RestClient.builder(HttpHost.create(uri)).build()) {
      Request request = new Request("PUT", "/" + INDEX);
      request.setJsonEntity(MAPPING);

      Response response = client.performRequest(request);
      assertEquals(200, response.getStatusLine().getStatusCode());
    }
  }
}
