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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Unstable;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pushes Gravitino roles and grants into the Elasticsearch security model, so that clients querying
 * Elasticsearch directly are subject to the privileges defined in Gravitino.
 *
 * <p>A Gravitino role becomes an Elasticsearch role carrying index privileges. A grant of that role
 * to a user or a group becomes a role mapping rule keyed on the username or on the groups claim of
 * the token the caller presents. Elasticsearch holds no users of its own in this model: identity
 * and group membership arrive from the identity provider at query time.
 */
@Unstable
public class ElasticsearchAuthorizationPlugin implements AuthorizationPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchAuthorizationPlugin.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String MAPPING_SUFFIX = "__mapping";

  private final String metalake;
  private final ElasticsearchAuthorizationProperties properties;
  private final ElasticsearchSecurityClient client;
  private final ElasticsearchRoleTranslator translator;
  private final ScheduledExecutorService reconciler;

  /**
   * Roles this plugin has applied, kept so that reconciliation can re-apply them and so that a deny
   * expansion picks up indices created since the grant.
   */
  private final Map<String, Role> appliedRoles = new ConcurrentHashMap<>();

  /**
   * Creates the plugin.
   *
   * @param metalake the metalake owning the catalog.
   * @param config the catalog properties.
   */
  public ElasticsearchAuthorizationPlugin(String metalake, Map<String, String> config) {
    this.metalake = metalake;
    this.properties = new ElasticsearchAuthorizationProperties(config);
    this.properties.validate();
    this.client = new ElasticsearchSecurityClient(properties);
    this.translator =
        new ElasticsearchRoleTranslator(properties.indexPrefix(), client::listIndices);

    warnOnLicense(client.licenseType());

    long interval = properties.reconcileIntervalSeconds();
    if (interval > 0) {
      this.reconciler =
          Executors.newSingleThreadScheduledExecutor(
              runnable -> {
                Thread thread = new Thread(runnable, "gravitino-es-authz-reconciler");
                thread.setDaemon(true);
                return thread;
              });
      this.reconciler.scheduleWithFixedDelay(
          this::reconcileQuietly, interval, interval, TimeUnit.SECONDS);
    } else {
      this.reconciler = null;
    }
  }

  @VisibleForTesting
  ElasticsearchAuthorizationPlugin(
      String metalake,
      ElasticsearchAuthorizationProperties properties,
      ElasticsearchSecurityClient client) {
    this.metalake = metalake;
    this.properties = properties;
    this.client = client;
    this.translator =
        new ElasticsearchRoleTranslator(properties.indexPrefix(), client::listIndices);
    this.reconciler = null;
  }

  // Roles

  @Override
  public Boolean onRoleCreated(Role role) throws AuthorizationPluginException {
    applyRole(role);
    return true;
  }

  @Override
  public Boolean onRoleAcquired(Role role) throws AuthorizationPluginException {
    return client.getRole(esRoleName(role.name())).isPresent();
  }

  @Override
  public Boolean onRoleDeleted(Role role) throws AuthorizationPluginException {
    String esRole = esRoleName(role.name());
    client.deleteRoleMapping(esRole + MAPPING_SUFFIX);
    client.deleteRole(esRole);
    appliedRoles.remove(role.name());
    return true;
  }

  @Override
  public Boolean onRoleUpdated(Role role, RoleChange... changes)
      throws AuthorizationPluginException {
    // Every change kind alters which securable objects the role carries, and the role passed in
    // already reflects them, so the role descriptor is rebuilt from the role as a whole. Writing
    // the whole descriptor is also what keeps this operation idempotent.
    for (RoleChange change : changes) {
      if (!(change instanceof RoleChange.AddSecurableObject)
          && !(change instanceof RoleChange.RemoveSecurableObject)
          && !(change instanceof RoleChange.UpdateSecurableObject)) {
        throw new IllegalArgumentException(
            String.format("RoleChange is not supported - %s", change));
      }
    }
    applyRole(role);
    invalidateKeysOfPrincipalsHolding(role);
    return true;
  }

  // Grants

  @Override
  public Boolean onGrantedRolesToUser(List<Role> roles, User user)
      throws AuthorizationPluginException {
    for (Role role : roles) {
      applyRole(role);
      updateRoleMapping(role, "username", user.name(), true);
    }
    return true;
  }

  @Override
  public Boolean onRevokedRolesFromUser(List<Role> roles, User user)
      throws AuthorizationPluginException {
    for (Role role : roles) {
      updateRoleMapping(role, "username", user.name(), false);
    }
    if (properties.invalidateApiKeysOnRevoke()) {
      client.invalidateApiKeysOf(user.name());
    }
    return true;
  }

  @Override
  public Boolean onGrantedRolesToGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException {
    for (Role role : roles) {
      applyRole(role);
      updateRoleMapping(role, "groups", group.name(), true);
    }
    return true;
  }

  @Override
  public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException {
    for (Role role : roles) {
      updateRoleMapping(role, "groups", group.name(), false);
    }
    return true;
  }

  // Users, groups and owners

  @Override
  public Boolean onUserAdded(User user) throws AuthorizationPluginException {
    // Elasticsearch holds no user records in this model. Identity arrives in the token presented at
    // query time and is resolved by role mappings.
    return true;
  }

  @Override
  public Boolean onUserRemoved(User user) throws AuthorizationPluginException {
    for (Role role : appliedRoles.values()) {
      updateRoleMapping(role, "username", user.name(), false);
    }
    if (properties.invalidateApiKeysOnRevoke()) {
      client.invalidateApiKeysOf(user.name());
    }
    return true;
  }

  @Override
  public Boolean onUserAcquired(User user) throws AuthorizationPluginException {
    return true;
  }

  @Override
  public Boolean onGroupAdded(Group group) throws AuthorizationPluginException {
    return true;
  }

  @Override
  public Boolean onGroupRemoved(Group group) throws AuthorizationPluginException {
    for (Role role : appliedRoles.values()) {
      updateRoleMapping(role, "groups", group.name(), false);
    }
    return true;
  }

  @Override
  public Boolean onGroupAcquired(Group group) throws AuthorizationPluginException {
    return true;
  }

  @Override
  public Boolean onOwnerSet(MetadataObject metadataObject, Owner preOwner, Owner newOwner)
      throws AuthorizationPluginException {
    // Elasticsearch has no per-object ownership, and ownership grants no index privilege on its
    // own, so there is nothing to push.
    return true;
  }

  // Metadata changes

  @Override
  public Boolean onMetadataUpdated(MetadataObjectChange... changes) throws RuntimeException {
    // A renamed or dropped index changes which literal index names the role descriptors name, so
    // every applied role is rewritten.
    boolean relevant = false;
    for (MetadataObjectChange change : changes) {
      if (change instanceof MetadataObjectChange.RenameMetadataObject
          || change instanceof MetadataObjectChange.RemoveMetadataObject) {
        relevant = true;
      }
    }
    if (relevant) {
      reconcile();
    }
    return true;
  }

  /**
   * Re-applies every role this plugin owns. Reconciliation reverts hand edits to the roles and
   * picks up indices created since a deny was expanded into an explicit allow list.
   */
  public void reconcile() {
    for (Role role : appliedRoles.values()) {
      applyRole(role);
    }
  }

  @Override
  public void close() throws IOException {
    if (reconciler != null) {
      reconciler.shutdownNow();
    }
    client.close();
  }

  private void applyRole(Role role) {
    ObjectNode descriptor = translator.translate(role, metalake);
    client.putRole(esRoleName(role.name()), descriptor);
    appliedRoles.put(role.name(), role);
    LOG.info(
        "Applied Gravitino role {} to Elasticsearch as {}", role.name(), esRoleName(role.name()));
  }

  /**
   * Adds or removes a principal from the role mapping that backs a Gravitino role. The mapping is
   * read back from Elasticsearch before it is rewritten, so the result does not depend on plugin
   * state surviving a restart.
   */
  private void updateRoleMapping(Role role, String field, String principal, boolean add) {
    String esRole = esRoleName(role.name());
    String mappingName = esRole + MAPPING_SUFFIX;

    Set<String> usernames = new LinkedHashSet<>();
    Set<String> groups = new LinkedHashSet<>();
    client
        .getRoleMapping(mappingName)
        .ifPresent(
            existing -> {
              readRule(existing, "username", usernames);
              readRule(existing, "groups", groups);
            });

    Set<String> target = "username".equals(field) ? usernames : groups;
    if (add) {
      target.add(principal);
    } else {
      target.remove(principal);
    }

    if (usernames.isEmpty() && groups.isEmpty()) {
      client.deleteRoleMapping(mappingName);
      return;
    }

    ObjectNode mapping = MAPPER.createObjectNode();
    mapping.put("enabled", true);
    mapping.putArray("roles").add(esRole);

    ObjectNode rules = mapping.putObject("rules");
    ArrayNode any = rules.putArray("any");
    if (!usernames.isEmpty()) {
      any.add(fieldRule("username", usernames));
    }
    if (!groups.isEmpty()) {
      any.add(fieldRule("groups", groups));
    }

    ObjectNode metadata = mapping.putObject("metadata");
    metadata.put("managed_by", "gravitino");
    metadata.put("gravitino_metalake", metalake);
    metadata.put("gravitino_role", role.name());

    client.putRoleMapping(mappingName, mapping);
  }

  private static ObjectNode fieldRule(String field, Set<String> values) {
    ObjectNode rule = MAPPER.createObjectNode();
    ArrayNode array = rule.putObject("field").putArray(field);
    values.forEach(array::add);
    return rule;
  }

  private static void readRule(JsonNode mapping, String field, Set<String> into) {
    JsonNode any = mapping.path("rules").path("any");
    for (JsonNode rule : any) {
      JsonNode value = rule.path("field").path(field);
      if (value.isArray()) {
        value.forEach(node -> into.add(node.asText()));
      } else if (value.isTextual()) {
        into.add(value.asText());
      }
    }
  }

  /**
   * Invalidates the API keys of every principal holding a role whose privileges just changed. A key
   * captures its owner's privileges when it is created, so a narrowed role does not otherwise reach
   * keys already issued.
   */
  private void invalidateKeysOfPrincipalsHolding(Role role) {
    if (!properties.invalidateApiKeysOnRevoke()) {
      return;
    }
    Set<String> usernames = new LinkedHashSet<>();
    client
        .getRoleMapping(esRoleName(role.name()) + MAPPING_SUFFIX)
        .ifPresent(mapping -> readRule(mapping, "username", usernames));
    usernames.forEach(client::invalidateApiKeysOf);
  }

  private void reconcileQuietly() {
    try {
      reconcile();
    } catch (RuntimeException e) {
      LOG.warn("Elasticsearch authorization reconciliation failed", e);
    }
  }

  private void warnOnLicense(String licenseType) {
    if ("basic".equals(licenseType)) {
      LOG.warn(
          "The Elasticsearch cluster reports a basic license. Index privileges are enforced, but "
              + "document-level and field-level security are not available, so row filters and "
              + "column restrictions in Gravitino roles will be rejected when applied.");
    } else {
      LOG.info("The Elasticsearch cluster reports license type {}", licenseType);
    }
  }

  /**
   * Returns the Elasticsearch role name backing a Gravitino role. The prefix marks the role as
   * owned by this plugin, and the metalake keeps two metalakes from colliding on one cluster.
   *
   * @param roleName the Gravitino role name.
   * @return the Elasticsearch role name.
   */
  @VisibleForTesting
  String esRoleName(String roleName) {
    return properties.rolePrefix() + sanitize(metalake) + "_" + sanitize(roleName);
  }

  private static String sanitize(String value) {
    return value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_.-]", "_");
  }
}
