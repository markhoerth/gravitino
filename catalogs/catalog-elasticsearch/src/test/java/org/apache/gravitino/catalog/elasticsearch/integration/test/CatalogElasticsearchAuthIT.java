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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.elasticsearch.ElasticsearchCatalogOperations;
import org.apache.gravitino.catalog.elasticsearch.ElasticsearchCatalogPropertiesMetadata;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
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
 * Covers basic authentication against a security enabled cluster.
 *
 * <p>{@code CatalogElasticsearchIT} runs with security switched off, which exercises the mapping
 * path but never sends a credential. The target deployment requires one, so the credentialed path,
 * and both ways of getting it wrong, are covered here.
 *
 * <p>TLS is deliberately not covered. The official image does not auto provision HTTP certificates
 * in this configuration, and generating them inside the test would add more moving parts than the
 * coverage is worth, so {@code elasticsearch.tls.skip-verify} is validated against a real cluster
 * instead.
 */
@EnabledIf("dockerAvailable")
public class CatalogElasticsearchAuthIT {

  /** The version under test, overridable so a run can check an older major. */
  private static final String VERSION = System.getProperty("elasticsearch.test.version", "9.2.4");

  private static final String IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:" + VERSION;

  private static final int ES_PORT = 9200;

  private static final String INDEX = "secured_fixture";

  private static final String USER = "elastic";

  private static final String PASSWORD = "changeme";

  private static final Namespace NAMESPACE = Namespace.of("metalake", "catalog", "default");

  private static final String MAPPING =
      "{\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"}}}}";

  private static GenericContainer<?> container;

  private static String uri;

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
   * Starts a single node cluster that demands a credential, over plain HTTP, and creates the
   * fixture index as the superuser.
   *
   * @throws Exception if the container does not start or the index cannot be created
   */
  @BeforeAll
  public static void startCluster() throws Exception {
    container =
        new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withExposedPorts(ES_PORT)
            .withEnv("discovery.type", "single-node")
            .withEnv("xpack.security.enabled", "true")
            .withEnv("xpack.security.http.ssl.enabled", "false")
            .withEnv("ELASTIC_PASSWORD", PASSWORD)
            .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
            .waitingFor(
                Wait.forHttp("/")
                    .withBasicCredentials(USER, PASSWORD)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(5)));
    container.start();

    uri = "http://" + container.getHost() + ":" + container.getMappedPort(ES_PORT);
    createFixtureIndex();
  }

  /** Stops the cluster. */
  @AfterAll
  public static void stopCluster() {
    if (container != null) {
      container.stop();
      container = null;
    }
  }

  /**
   * A catalog carrying the right credential reads the cluster.
   *
   * @throws Exception if the catalog cannot be closed
   */
  @Test
  public void testAuthenticatedListTables() throws Exception {
    try (ElasticsearchCatalogOperations operations = catalog(USER, PASSWORD)) {
      Set<String> tables =
          Arrays.stream(operations.listTables(NAMESPACE))
              .map(NameIdentifier::name)
              .collect(Collectors.toCollection(TreeSet::new));

      assertTrue(tables.contains(INDEX), "listTables did not report " + INDEX + ", got " + tables);
    }
  }

  /**
   * A catalog carrying no credential fails outright rather than hanging or reporting an empty
   * cluster.
   *
   * <p>The reason the cluster gave does reach the caller, but not from {@code getMessage()}: {@code
   * listTables} wraps the transport failure in an {@link UncheckedIOException} whose own message is
   * a fixed string, so the cluster's own words are only on the cause. An operator reading a stack
   * trace sees them; an operator reading only the top-level message does not.
   *
   * @throws Exception if the catalog cannot be closed
   */
  @Test
  public void testMissingCredentialsFails() throws Exception {
    try (ElasticsearchCatalogOperations operations = catalog(null, null)) {
      UncheckedIOException thrown =
          assertThrows(UncheckedIOException.class, () -> operations.listTables(NAMESPACE));

      assertEquals("Failed to list Elasticsearch indices", thrown.getMessage());
      assertNotNull(thrown.getCause(), "the transport failure was dropped");

      String reason = thrown.getCause().getMessage();
      assertTrue(reason.contains("401"), "expected an unauthorized status in " + reason);
      assertTrue(
          reason.contains("missing authentication credentials"),
          "expected a missing credential reason in " + reason);
    }
  }

  /**
   * A wrong password fails differently from no password at all, so an operator can tell a
   * misconfigured credential from an absent one.
   *
   * @throws Exception if the catalog cannot be closed
   */
  @Test
  public void testWrongPasswordFails() throws Exception {
    try (ElasticsearchCatalogOperations operations = catalog(USER, "not-the-password")) {
      UncheckedIOException thrown =
          assertThrows(UncheckedIOException.class, () -> operations.listTables(NAMESPACE));

      String reason = thrown.getCause().getMessage();
      assertTrue(reason.contains("401"), "expected an unauthorized status in " + reason);
      assertTrue(
          reason.contains("unable to authenticate user [" + USER + "]"),
          "expected a rejected credential reason in " + reason);
      assertTrue(
          !reason.contains("missing authentication credentials"),
          "a wrong password was reported as a missing one: " + reason);
    }
  }

  /** A catalog pointed at the container, with the given credential or none at all. */
  private static ElasticsearchCatalogOperations catalog(String username, String password) {
    ImmutableMap.Builder<String, String> conf = ImmutableMap.builder();
    conf.put(ElasticsearchCatalogPropertiesMetadata.HOSTS, uri);
    if (username != null) {
      conf.put(ElasticsearchCatalogPropertiesMetadata.USERNAME, username);
      conf.put(ElasticsearchCatalogPropertiesMetadata.PASSWORD, password);
    }

    ElasticsearchCatalogOperations operations = new ElasticsearchCatalogOperations();
    operations.initialize(conf.build(), null, null);
    return operations;
  }

  /** Creates the fixture index as the superuser, through the client the connector itself uses. */
  private static void createFixtureIndex() throws Exception {
    CredentialsProvider credentials = new BasicCredentialsProvider();
    credentials.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(USER, PASSWORD));

    try (RestClient client =
        RestClient.builder(HttpHost.create(uri))
            .setHttpClientConfigCallback(
                builder -> builder.setDefaultCredentialsProvider(credentials))
            .build()) {
      Request request = new Request("PUT", "/" + INDEX);
      request.setJsonEntity(MAPPING);

      Response response = client.performRequest(request);
      assertEquals(200, response.getStatusLine().getStatusCode());
    }
  }
}
