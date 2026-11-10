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
package org.apache.gravitino.authorization.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.junit.jupiter.api.Test;

/** Tests the translation of Gravitino roles into Elasticsearch role descriptors. */
public class TestElasticsearchRoleTranslator {

  private static final List<String> CLUSTER_INDICES =
      ImmutableList.of("careiq_claims", "careiq_members", "careiq_pii", "reference_codes");

  // The existing cases exercise privilege translation on its own, with the usage gate off.
  private final ElasticsearchRoleTranslator translator =
      new ElasticsearchRoleTranslator("", () -> new ArrayList<>(CLUSTER_INDICES), false);

  private final ElasticsearchRoleTranslator gated =
      new ElasticsearchRoleTranslator("", () -> new ArrayList<>(CLUSTER_INDICES), true);

  @Test
  public void testTableSelectBecomesReadOnExactIndex() {
    SecurableObject table =
        SecurableObjects.ofTable(
            SecurableObjects.ofSchema(
                SecurableObjects.ofCatalog("es", ImmutableList.of()),
                "default",
                ImmutableList.of()),
            "careiq_claims",
            ImmutableList.of(Privileges.SelectTable.allow()));
    ObjectNode descriptor = translator.translate(role("reader", ImmutableList.of(table)), "prod");

    JsonNode indices = descriptor.get("indices");
    assertEquals(1, indices.size());
    assertEquals("careiq_claims", indices.get(0).get("names").get(0).asText());
    assertTrue(privilegesOf(indices.get(0)).contains("read"));
    assertTrue(privilegesOf(indices.get(0)).contains("view_index_metadata"));
  }

  @Test
  public void testCatalogGrantBecomesWildcardPattern() {
    SecurableObject catalog =
        SecurableObjects.ofCatalog("es", ImmutableList.of(Privileges.SelectTable.allow()));
    ObjectNode descriptor = translator.translate(role("broad", ImmutableList.of(catalog)), "prod");

    assertEquals("*", descriptor.get("indices").get(0).get("names").get(0).asText());
  }

  @Test
  public void testIndexPrefixScopesCatalogGrant() {
    ElasticsearchRoleTranslator prefixed =
        new ElasticsearchRoleTranslator("careiq_", () -> new ArrayList<>(CLUSTER_INDICES), false);
    SecurableObject catalog =
        SecurableObjects.ofCatalog("es", ImmutableList.of(Privileges.SelectTable.allow()));
    ObjectNode descriptor = prefixed.translate(role("broad", ImmutableList.of(catalog)), "prod");

    assertEquals("careiq_*", descriptor.get("indices").get(0).get("names").get(0).asText());
  }

  @Test
  public void testDenyUnderAllowExpandsPatternIntoExplicitIndices() {
    SecurableObject catalog =
        SecurableObjects.ofCatalog("es", ImmutableList.of(Privileges.SelectTable.allow()));
    SecurableObject denied =
        SecurableObjects.ofTable(
            SecurableObjects.ofSchema(
                SecurableObjects.ofCatalog("es", ImmutableList.of()),
                "default",
                ImmutableList.of()),
            "careiq_pii",
            ImmutableList.of(Privileges.SelectTable.deny()));

    ObjectNode descriptor =
        translator.translate(role("most", ImmutableList.of(catalog, denied)), "prod");

    List<String> names = new ArrayList<>();
    descriptor.get("indices").forEach(entry -> names.add(entry.get("names").get(0).asText()));

    assertEquals(3, names.size());
    assertTrue(
        names.containsAll(Arrays.asList("careiq_claims", "careiq_members", "reference_codes")));
    assertFalse(names.contains("careiq_pii"));
    assertFalse(names.contains("*"));
  }

  @Test
  public void testUnsupportedPrivilegeIsSkipped() {
    SecurableObject catalog =
        SecurableObjects.ofCatalog("es", ImmutableList.of(Privileges.CreateRole.allow()));
    ObjectNode descriptor = translator.translate(role("none", ImmutableList.of(catalog)), "prod");

    assertEquals(0, descriptor.get("indices").size());
  }

  @Test
  public void testSelectWithoutUsagePrivilegesIsNotPushed() {
    SecurableObject catalog =
        SecurableObjects.ofCatalog("es", ImmutableList.of(Privileges.SelectTable.allow()));
    ObjectNode descriptor = gated.translate(role("reader", ImmutableList.of(catalog)), "prod");

    assertEquals(
        0,
        descriptor.get("indices").size(),
        "Gravitino refuses a table read without USE_CATALOG and USE_SCHEMA");
  }

  @Test
  public void testSelectWithUsagePrivilegesIsPushed() {
    SecurableObject catalog =
        SecurableObjects.ofCatalog(
            "es",
            ImmutableList.of(
                Privileges.UseCatalog.allow(),
                Privileges.UseSchema.allow(),
                Privileges.SelectTable.allow()));
    ObjectNode descriptor = gated.translate(role("reader", ImmutableList.of(catalog)), "prod");

    assertEquals(1, descriptor.get("indices").size());
    assertTrue(privilegesOf(descriptor.get("indices").get(0)).contains("read"));
  }

  @Test
  public void testUsageOnSchemaSatisfiesTheSchemaHalfOfTheGate() {
    SecurableObject catalog =
        SecurableObjects.ofCatalog("es", ImmutableList.of(Privileges.UseCatalog.allow()));
    SecurableObject schema =
        SecurableObjects.ofSchema(
            SecurableObjects.ofCatalog("es", ImmutableList.of()),
            "default",
            ImmutableList.of(Privileges.UseSchema.allow(), Privileges.SelectTable.allow()));

    ObjectNode descriptor =
        gated.translate(role("reader", ImmutableList.of(catalog, schema)), "prod");

    assertTrue(descriptor.get("indices").size() > 0);
  }

  @Test
  public void testUsagePrivilegesAloneStillGrantVisibility() {
    SecurableObject catalog =
        SecurableObjects.ofCatalog(
            "es", ImmutableList.of(Privileges.UseCatalog.allow(), Privileges.UseSchema.allow()));
    ObjectNode descriptor = gated.translate(role("browser", ImmutableList.of(catalog)), "prod");

    List<String> privileges = privilegesOf(descriptor.get("indices").get(0));
    assertTrue(privileges.contains("view_index_metadata"));
    assertFalse(privileges.contains("read"));
  }

  private static List<String> privilegesOf(JsonNode entry) {
    List<String> privileges = new ArrayList<>();
    entry.get("privileges").forEach(node -> privileges.add(node.asText()));
    return privileges;
  }

  private static Role role(String name, List<SecurableObject> objects) {
    return new Role() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public Map<String, String> properties() {
        return ImmutableMap.of();
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
}
