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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Audit;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests how the plugin drives Elasticsearch roles and role mappings. */
public class TestElasticsearchAuthorizationPlugin {

  private FakeSecurityClient client;
  private ElasticsearchAuthorizationPlugin plugin;

  @BeforeEach
  public void setUp() {
    Map<String, String> config = new HashMap<>();
    config.put(ElasticsearchAuthorizationProperties.URL, "https://es.example.com:9200");
    config.put(ElasticsearchAuthorizationProperties.AUTH_TYPE, "basic");
    config.put(ElasticsearchAuthorizationProperties.USERNAME, "elastic");
    config.put(ElasticsearchAuthorizationProperties.PASSWORD, "secret");

    ElasticsearchAuthorizationProperties properties =
        new ElasticsearchAuthorizationProperties(config);
    properties.validate();
    client = new FakeSecurityClient();
    plugin = new ElasticsearchAuthorizationPlugin("prod", properties, client);
  }

  @Test
  public void testRoleNameCarriesPrefixAndMetalake() {
    assertEquals("gravitino_prod_claims_reader", plugin.esRoleName("Claims Reader"));
  }

  @Test
  public void testGrantToUserAndGroupShareOneMapping() {
    Role role = readerRole();
    plugin.onGrantedRolesToUser(ImmutableList.of(role), user("mhoerth"));
    plugin.onGrantedRolesToGroup(ImmutableList.of(role), group("claims-analysts"));

    ObjectNode mapping = client.roleMappings.get("gravitino_prod_reader__mapping");
    assertEquals("gravitino_prod_reader", mapping.get("roles").get(0).asText());

    JsonNode any = mapping.get("rules").get("any");
    assertEquals(2, any.size());
    assertTrue(ruleValues(mapping, "username").contains("mhoerth"));
    assertTrue(ruleValues(mapping, "groups").contains("claims-analysts"));
  }

  @Test
  public void testRevokeLeavesTheOtherPrincipalInPlace() {
    Role role = readerRole();
    plugin.onGrantedRolesToUser(ImmutableList.of(role), user("mhoerth"));
    plugin.onGrantedRolesToGroup(ImmutableList.of(role), group("claims-analysts"));
    plugin.onRevokedRolesFromUser(ImmutableList.of(role), user("mhoerth"));

    ObjectNode mapping = client.roleMappings.get("gravitino_prod_reader__mapping");
    assertFalse(ruleValues(mapping, "username").contains("mhoerth"));
    assertTrue(ruleValues(mapping, "groups").contains("claims-analysts"));
  }

  @Test
  public void testMappingIsDeletedWhenNoPrincipalsRemain() {
    Role role = readerRole();
    plugin.onGrantedRolesToUser(ImmutableList.of(role), user("mhoerth"));
    plugin.onRevokedRolesFromUser(ImmutableList.of(role), user("mhoerth"));

    assertFalse(client.roleMappings.containsKey("gravitino_prod_reader__mapping"));
  }

  @Test
  public void testRevokeInvalidatesTheUsersApiKeys() {
    Role role = readerRole();
    plugin.onGrantedRolesToUser(ImmutableList.of(role), user("mhoerth"));
    plugin.onRevokedRolesFromUser(ImmutableList.of(role), user("mhoerth"));

    assertTrue(client.invalidatedKeyOwners.contains("mhoerth"));
  }

  @Test
  public void testDeletingARoleRemovesTheRoleAndItsMapping() {
    Role role = readerRole();
    plugin.onGrantedRolesToUser(ImmutableList.of(role), user("mhoerth"));
    plugin.onRoleDeleted(role);

    assertFalse(client.roles.containsKey("gravitino_prod_reader"));
    assertFalse(client.roleMappings.containsKey("gravitino_prod_reader__mapping"));
  }

  @Test
  public void testReconcileRewritesEveryAppliedRole() {
    Role role = readerRole();
    plugin.onRoleCreated(role);
    client.roles.clear();
    plugin.reconcile();

    assertTrue(client.roles.containsKey("gravitino_prod_reader"));
  }

  private static List<String> ruleValues(ObjectNode mapping, String field) {
    List<String> values = new ArrayList<>();
    mapping
        .get("rules")
        .get("any")
        .forEach(
            rule -> {
              JsonNode node = rule.path("field").path(field);
              node.forEach(value -> values.add(value.asText()));
            });
    return values;
  }

  private static Role readerRole() {
    SecurableObject catalog =
        SecurableObjects.ofCatalog("es", ImmutableList.of(Privileges.SelectTable.allow()));
    return new Role() {
      @Override
      public String name() {
        return "reader";
      }

      @Override
      public Map<String, String> properties() {
        return ImmutableMap.of();
      }

      @Override
      public List<SecurableObject> securableObjects() {
        return ImmutableList.of(catalog);
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

  /** An in-memory stand-in for the Elasticsearch security API. */
  private static class FakeSecurityClient extends ElasticsearchSecurityClient {

    private final Map<String, ObjectNode> roles = new HashMap<>();
    private final Map<String, ObjectNode> roleMappings = new HashMap<>();
    private final List<String> invalidatedKeyOwners = new ArrayList<>();

    FakeSecurityClient() {
      super(null, null, null, null);
    }

    @Override
    public void putRole(String roleName, ObjectNode roleDescriptor) {
      roles.put(roleName, roleDescriptor);
    }

    @Override
    public void deleteRole(String roleName) {
      roles.remove(roleName);
    }

    @Override
    public Optional<JsonNode> getRole(String roleName) {
      return Optional.ofNullable(roles.get(roleName));
    }

    @Override
    public void putRoleMapping(String mappingName, ObjectNode mapping) {
      roleMappings.put(mappingName, mapping);
    }

    @Override
    public void deleteRoleMapping(String mappingName) {
      roleMappings.remove(mappingName);
    }

    @Override
    public Optional<JsonNode> getRoleMapping(String mappingName) {
      return Optional.ofNullable(roleMappings.get(mappingName));
    }

    @Override
    public List<String> listIndices() {
      return ImmutableList.of("careiq_claims", "careiq_pii");
    }

    @Override
    public String licenseType() {
      return "trial";
    }

    @Override
    public void invalidateApiKeysOf(String username) {
      invalidatedKeyOwners.add(username);
    }

    @Override
    public void close() {}
  }
}
