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
package org.apache.gravitino.authorization.kafka;

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.utils.NamespaceUtil;

/**
 * Reads the users, groups and roles of one metalake from Gravitino and returns the effective Topic
 * grants that apply to one catalog.
 */
public class GravitinoEffectiveGrantSource implements EffectiveGrantSource {

  private static final Set<Privilege.Name> TOPIC_PRIVILEGES =
      ImmutableSet.of(
          Privilege.Name.CONSUME_TOPIC, Privilege.Name.PRODUCE_TOPIC, Privilege.Name.CREATE_TOPIC);

  private final String metalake;
  private final long catalogId;
  private final KafkaPrincipalResolver resolver;

  /**
   * Creates a grant source. Gravitino is not accessed until {@link #grants} is called.
   *
   * @param metalake the metalake name
   * @param catalogId the stable entity ID of the catalog this plugin serves
   * @param resolver maps Gravitino users and groups to Kafka Principals
   */
  public GravitinoEffectiveGrantSource(
      String metalake, long catalogId, KafkaPrincipalResolver resolver) {
    this.metalake = metalake;
    this.catalogId = catalogId;
    this.resolver = resolver;
  }

  @Override
  public Collection<KafkaGrant> grants(PendingRemovals pending) {
    AccessControlDispatcher dispatcher = GravitinoEnv.getInstance().accessControlDispatcher();
    if (dispatcher == null) {
      throw new AuthorizationPluginException(
          "Gravitino access control is not enabled; cannot read grants for metalake %s", metalake);
    }
    if (pending.removesCatalog()) {
      return List.of();
    }
    String catalog = catalogName();

    Map<String, Set<String>> rolesByPrincipal = new HashMap<>();
    for (User user : dispatcher.listUsers(metalake)) {
      if (pending.removesUser(user.name())) {
        continue;
      }
      resolver
          .resolveUser(user.name())
          .ifPresent(
              p -> rolesByPrincipal.computeIfAbsent(p, k -> new HashSet<>()).addAll(user.roles()));
    }
    for (Group group : dispatcher.listGroups(metalake)) {
      if (pending.removesGroup(group.name()) || group.roles().isEmpty()) {
        continue;
      }
      for (String principal : resolver.resolveGroup(group.name())) {
        rolesByPrincipal.computeIfAbsent(principal, k -> new HashSet<>()).addAll(group.roles());
      }
    }

    Map<String, List<SecurableObject>> objectsByRole = new HashMap<>();
    List<KafkaGrant> grants = new ArrayList<>();
    for (Map.Entry<String, Set<String>> entry : rolesByPrincipal.entrySet()) {
      for (String role : entry.getValue()) {
        if (pending.removesRole(role)) {
          continue;
        }
        List<SecurableObject> objects =
            objectsByRole.computeIfAbsent(role, r -> securableObjects(dispatcher, r));
        for (SecurableObject object : objects) {
          addGrants(entry.getKey(), object, catalog, pending, grants);
        }
      }
    }
    return KafkaGrant.effective(grants);
  }

  private void addGrants(
      String principal,
      SecurableObject object,
      String catalog,
      PendingRemovals pending,
      List<KafkaGrant> grants) {
    KafkaGrant.Scope scope;
    String resource;
    switch (object.type()) {
      case METALAKE:
        scope = KafkaGrant.Scope.METALAKE;
        resource = object.fullName();
        break;
      case CATALOG:
        if (!object.fullName().equals(catalog)) {
          return;
        }
        scope = KafkaGrant.Scope.CATALOG;
        resource = object.fullName();
        break;
      case SCHEMA:
        if (!object.fullName().startsWith(catalog + ".")) {
          return;
        }
        scope = KafkaGrant.Scope.SCHEMA;
        resource = object.fullName();
        break;
      case TOPIC:
        if (!object.fullName().startsWith(catalog + ".")
            || pending.removesTopic(object.fullName())) {
          return;
        }
        scope = KafkaGrant.Scope.TOPIC;
        resource = object.name();
        break;
      default:
        return;
    }

    for (Privilege privilege : object.privileges()) {
      if (TOPIC_PRIVILEGES.contains(privilege.name())) {
        KafkaGrant.Effect effect =
            privilege.condition() == Privilege.Condition.DENY
                ? KafkaGrant.Effect.DENY
                : KafkaGrant.Effect.ALLOW;
        grants.add(new KafkaGrant(principal, scope, resource, privilege.name(), effect));
      }
    }
  }

  private List<SecurableObject> securableObjects(AccessControlDispatcher dispatcher, String role) {
    try {
      return dispatcher.getRole(metalake, role).securableObjects();
    } catch (NoSuchRoleException e) {
      // The role was deleted between listing the user and reading the role.
      return List.of();
    }
  }

  private String catalogName() {
    // Resolved on every call so that a catalog rename is picked up by the next reconcile.
    try {
      return GravitinoEnv.getInstance()
          .entityStore()
          .list(NamespaceUtil.ofCatalog(metalake), CatalogEntity.class, Entity.EntityType.CATALOG)
          .stream()
          .filter(c -> c.id() == catalogId)
          .map(CatalogEntity::name)
          .findFirst()
          .orElseThrow(
              () ->
                  new AuthorizationPluginException(
                      "Catalog with ID %s not found in metalake %s", catalogId, metalake));
    } catch (IOException e) {
      throw new AuthorizationPluginException(
          e,
          "Failed to resolve catalog ID %s in metalake %s: %s",
          catalogId,
          metalake,
          e.getMessage());
    }
  }
}
