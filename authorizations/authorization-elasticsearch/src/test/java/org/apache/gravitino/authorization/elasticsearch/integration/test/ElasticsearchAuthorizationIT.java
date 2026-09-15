/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.authorization.elasticsearch.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.authorization.elasticsearch.ElasticsearchAuthorizationPlugin;
import org.apache.gravitino.authorization.elasticsearch.ElasticsearchAuthorizationProperties;
import org.apache.gravitino.authorization.elasticsearch.ElasticsearchRoleTranslator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Drives the Elasticsearch authorization plugin against a live cluster, which is what exercises the
 * security client over real HTTP and the deny expansion against a real index list.
 *
 * <p>The test is skipped unless {@code ES_URL} is set. To run it against a local container:
 *
 * <pre>
 * docker run -d --name es-authz -p 9200:9200 \
 *   -e discovery.type=single-node -e xpack.security.enabled=true \
 *   -e xpack.security.http.ssl.enabled=false -e ELASTIC_PASSWORD=smoketest1 \
 *   docker.elastic.co/elasticsearch/elasticsearch:8.15.0
 * curl -u elastic:smoketest1 -X POST "http://localhost:9200/_license/start_trial?acknowledge=true"
 *
 * ES_URL=http://localhost:9200 ES_USER=elastic ES_PASSWORD=smoketest1 \
 *   ./gradlew :authorizations:authorization-elasticsearch:test --tests "*ElasticsearchAuthorizationIT"
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "ES_URL", matches = ".+")
public class ElasticsearchAuthorizationIT {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String METALAKE = "it";
  private static final String CLAIMS = "it_claims";
  private static final String PII = "it_pii";
  private static final String REFERENCE = "it_reference";
  private static final String NATIVE_USER = "gravitino_it_user";
  private static final String NATIVE_PASSWORD = "gravitino_it_password_1";

  private static String esUrl;
  private static String adminAuth;
  private static HttpClient httpClient;
  private static ElasticsearchAuthorizationPlugin plugin;
  private static boolean paidTier;

  @BeforeAll
  public static void setUp() throws Exception {
    esUrl = System.getenv("ES_URL");
    String user = System.getenv().getOrDefault("ES_USER", "elastic");
    String password = System.getenv("ES_PASSWORD");
    adminAuth =
        "Basic "
            + Base64.getEncoder()
                .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    JsonNode license = MAPPER.readTree(admin("GET", "/_license", null));
    paidTier = !"basic".equals(license.path("license").path("type").asText());

    createIndex(CLAIMS, "{\"region\":{\"type\":\"keyword\"},\"ssn\":{\"type\":\"keyword\"}}");
    createIndex(PII, "{\"ssn\":{\"type\":\"keyword\"}}");
    createIndex(REFERENCE, "{\"code\":{\"type\":\"keyword\"}}");
    admin("POST", "/" + CLAIMS + "/_doc?refresh=true", "{\"region\":\"west\",\"ssn\":\"1\"}");
    admin("POST", "/" + CLAIMS + "/_doc?refresh=true", "{\"region\":\"east\",\"ssn\":\"2\"}");
    admin("POST", "/" + PII + "/_doc?refresh=true", "{\"ssn\":\"3\"}");
    admin("POST", "/" + REFERENCE + "/_doc?refresh=true", "{\"code\":\"A\"}");

    Map<String, String> config = new HashMap<>();
    config.put(ElasticsearchAuthorizationProperties.URL, esUrl);
    config.put(ElasticsearchAuthorizationProperties.AUTH_TYPE, "basic");
    config.put(ElasticsearchAuthorizationProperties.USERNAME, user);
    config.put(ElasticsearchAuthorizationProperties.PASSWORD, password);
    config.put(ElasticsearchAuthorizationProperties.INDEX_PREFIX, "it_");
    // Reconciliation is driven explicitly in these tests.
    config.put(ElasticsearchAuthorizationProperties.RECONCILE_INTERVAL_SECONDS, "0");
    plugin = new ElasticsearchAuthorizationPlugin(METALAKE, config);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (plugin != null) {
      plugin.close();
    }
    silently("DELETE", "/_security/user/" + NATIVE_USER);
    silently("DELETE", "/" + CLAIMS);
    silently("DELETE", "/" + PII);
    silently("DELETE", "/" + REFERENCE);
    for (String role : new String[] {"reader", "restricted", "regional"}) {
      silently("DELETE", "/_security/role/gravitino_it_" + role);
      silently("DELETE", "/_security/role_mapping/gravitino_it_" + role + "__mapping");
    }
  }

  @Test
  public void testRoleAndMappingLandOnTheCluster() throws Exception {
    resetRole("reader");
    Role role = role("reader", ImmutableList.of(catalogSelect()), ImmutableMap.of());
    plugin.onRoleCreated(role);
    plugin.onGrantedRolesToUser(ImmutableList.of(role), user("mhoerth"));
    plugin.onGrantedRolesToGroup(ImmutableList.of(role), group("analysts"));

    JsonNode esRole = fetch("/_security/role/gravitino_it_reader").get("gravitino_it_reader");
    assertEquals("it_*", esRole.get("indices").get(0).get("names").get(0).asText());
    assertEquals("gravitino", esRole.path("metadata").path("managed_by").asText());

    JsonNode mapping =
        fetch("/_security/role_mapping/gravitino_it_reader__mapping")
            .get("gravitino_it_reader__mapping");
    assertEquals("gravitino_it_reader", mapping.get("roles").get(0).asText());
    assertTrue(ruleValues(mapping, "username").contains("mhoerth"));
    assertTrue(ruleValues(mapping, "groups").contains("analysts"));
  }

  @Test
  public void testDenyExpandsAgainstTheLiveIndexList() throws Exception {
    resetRole("reader");
    Role role =
        role("reader", ImmutableList.of(catalogSelect(), tableDeny(PII)), ImmutableMap.of());
    plugin.onRoleCreated(role);

    List<String> names = new ArrayList<>();
    fetch("/_security/role/gravitino_it_reader")
        .get("gravitino_it_reader")
        .get("indices")
        .forEach(entry -> entry.get("names").forEach(name -> names.add(name.asText())));

    assertTrue(names.contains(CLAIMS));
    assertTrue(names.contains(REFERENCE));
    assertFalse(names.contains(PII));
    assertFalse(names.contains("it_*"));
  }

  @Test
  public void testFieldRestrictionIsEnforcedForARealQuery() throws Exception {
    resetRole("restricted");
    if (!paidTier) {
      return;
    }
    Role role =
        role(
            "restricted",
            ImmutableList.of(catalogSelect()),
            ImmutableMap.of(ElasticsearchRoleTranslator.FLS_PROPERTY_PREFIX + "*", "ssn"));
    plugin.onRoleCreated(role);
    createNativeUser("gravitino_it_restricted");

    String result = asNativeUser("/" + CLAIMS + "/_search");
    assertTrue(result.contains("region"));
    assertFalse(result.contains("\"ssn\""));
  }

  @Test
  public void testRowFilterIsEnforcedForARealQuery() throws Exception {
    resetRole("regional");
    if (!paidTier) {
      return;
    }
    Role role =
        role(
            "regional",
            ImmutableList.of(catalogSelect()),
            ImmutableMap.of(
                ElasticsearchRoleTranslator.DLS_PROPERTY_PREFIX + "*",
                "{\"term\":{\"region\":\"west\"}}"));
    plugin.onRoleCreated(role);
    createNativeUser("gravitino_it_regional");

    String result = asNativeUser("/" + CLAIMS + "/_search");
    assertTrue(result.contains("west"));
    assertFalse(result.contains("east"));
  }

  @Test
  public void testRevokeRemovesTheMappingAndRoleDeletionRemovesBoth() throws Exception {
    resetRole("reader");
    Role role = role("reader", ImmutableList.of(catalogSelect()), ImmutableMap.of());
    plugin.onRoleCreated(role);
    plugin.onGrantedRolesToUser(ImmutableList.of(role), user("mhoerth"));
    plugin.onRevokedRolesFromUser(ImmutableList.of(role), user("mhoerth"));

    assertEquals(404, status("GET", "/_security/role_mapping/gravitino_it_reader__mapping"));

    plugin.onRoleDeleted(role);
    assertEquals(404, status("GET", "/_security/role/gravitino_it_reader"));
  }

  // Helpers

  /** Removes any role and mapping left behind by another test using the same role name. */
  private static void resetRole(String name) throws Exception {
    silently("DELETE", "/_security/role_mapping/gravitino_it_" + name + "__mapping");
    silently("DELETE", "/_security/role/gravitino_it_" + name);
  }

  private static SecurableObject catalogSelect() {
    return SecurableObjects.ofCatalog("es", ImmutableList.of(Privileges.SelectTable.allow()));
  }

  private static SecurableObject tableDeny(String index) {
    SecurableObject schema =
        SecurableObjects.ofSchema(
            SecurableObjects.ofCatalog("es", ImmutableList.of()), "default", ImmutableList.of());
    return SecurableObjects.ofTable(schema, index, ImmutableList.of(Privileges.SelectTable.deny()));
  }

  private static void createIndex(String name, String properties) throws Exception {
    silently("DELETE", "/" + name);
    admin("PUT", "/" + name, "{\"mappings\":{\"properties\":" + properties + "}}");
  }

  private static void createNativeUser(String esRole) throws Exception {
    admin(
        "PUT",
        "/_security/user/" + NATIVE_USER,
        "{\"password\":\"" + NATIVE_PASSWORD + "\",\"roles\":[\"" + esRole + "\"]}");
  }

  private static String asNativeUser(String path) throws Exception {
    String credentials =
        Base64.getEncoder()
            .encodeToString((NATIVE_USER + ":" + NATIVE_PASSWORD).getBytes(StandardCharsets.UTF_8));
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(esUrl + path))
            .header("Authorization", "Basic " + credentials)
            .header("Content-Type", "application/json")
            .GET()
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body();
  }

  private static JsonNode fetch(String path) throws Exception {
    return MAPPER.readTree(admin("GET", path, null));
  }

  private static List<String> ruleValues(JsonNode mapping, String field) {
    List<String> values = new ArrayList<>();
    mapping
        .path("rules")
        .path("any")
        .forEach(
            rule -> {
              JsonNode node = rule.path("field").path(field);
              if (node.isTextual()) {
                values.add(node.asText());
              } else {
                node.forEach(v -> values.add(v.asText()));
              }
            });
    return values;
  }

  private static String admin(String method, String path, String body) throws Exception {
    HttpResponse<String> response = request(method, path, body);
    if (response.statusCode() >= 300) {
      throw new IOException(
          "Elasticsearch " + method + " " + path + " returned " + response.statusCode());
    }
    return response.body();
  }

  private static int status(String method, String path) throws Exception {
    return request(method, path, null).statusCode();
  }

  private static void silently(String method, String path) throws Exception {
    request(method, path, null);
  }

  private static HttpResponse<String> request(String method, String path, String body)
      throws Exception {
    HttpRequest.BodyPublisher publisher =
        body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8);
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(esUrl + path))
            .header("Authorization", adminAuth)
            .header("Content-Type", "application/json")
            .method(method, publisher)
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static Role role(
      String name, List<SecurableObject> objects, Map<String, String> properties) {
    return new Role() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public Map<String, String> properties() {
        return properties;
      }

      @Override
      public List<SecurableObject> securableObjects() {
        return objects;
      }

      @Override
      public Audit auditInfo() {
        return null;
      }
    };
  }

  private static User user(String name) {
    return new User() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public List<String> roles() {
        return ImmutableList.of();
      }

      @Override
      public Audit auditInfo() {
        return null;
      }
    };
  }

  private static Group group(String name) {
    return new Group() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public List<String> roles() {
        return ImmutableList.of();
      }

      @Override
      public Audit auditInfo() {
        return null;
      }
    };
  }
}
