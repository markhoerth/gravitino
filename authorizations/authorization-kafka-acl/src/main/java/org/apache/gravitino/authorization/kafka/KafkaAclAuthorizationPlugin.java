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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.RoleChange;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.connector.authorization.AuthorizationPlugin;
import org.apache.gravitino.connector.authorization.BaseAuthorization;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Materializes Gravitino Topic privileges as native Kafka ACLs.
 *
 * <p>Every hook runs one idempotent {@link #reconcile}: the desired ACLs are translated from the
 * effective grants, the actual ACLs are read from the cluster, and the difference is applied. Only
 * ACLs whose Principal starts with {@code authorization.kafka.principal-prefix} are ever created or
 * deleted; that prefix is the ownership rule of this plugin.
 */
public class KafkaAclAuthorizationPlugin implements AuthorizationPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAclAuthorizationPlugin.class);

  /** The state of the plugin. */
  public enum Health {
    /** Hooks reconcile normally. */
    HEALTHY,
    /** {@link KafkaAclProperties#RECONCILE_PAUSED} is set; hooks skip Kafka. */
    PAUSED
  }

  private final KafkaAclProperties properties;
  private final KafkaPrincipalResolver resolver;
  private final KafkaAclTranslator translator;
  private final EffectiveGrantSource grantSource;
  private final Admin admin;
  // A catalog property change rebuilds the catalog wrapper and with it this plugin, so the flag
  // is fixed for the lifetime of the instance.
  private final boolean paused;

  /**
   * Creates a plugin that reads grants from Gravitino.
   *
   * @param metalake the metalake name
   * @param config the catalog configuration, including {@link BaseAuthorization#CATALOG_ID}
   */
  public KafkaAclAuthorizationPlugin(String metalake, Map<String, String> config) {
    this(metalake, config, gravitinoGrantSource(metalake, config));
  }

  /**
   * Creates a plugin with a custom grant source and an AdminClient built from the configuration.
   *
   * @param metalake the metalake name
   * @param config the catalog configuration
   * @param grantSource supplies the effective grants
   */
  public KafkaAclAuthorizationPlugin(
      String metalake, Map<String, String> config, EffectiveGrantSource grantSource) {
    this(metalake, config, grantSource, createAdmin(config));
  }

  @VisibleForTesting
  KafkaAclAuthorizationPlugin(
      String metalake, Map<String, String> config, EffectiveGrantSource grantSource, Admin admin) {
    this.properties = validatedProperties(config);
    this.resolver = KafkaPrincipalResolver.withoutGroupMembership(properties.principalPrefix());
    this.translator = new KafkaAclTranslator(metalake, properties.groupPrefixTemplate());
    this.grantSource = grantSource;
    this.admin = admin;
    this.paused = properties.reconcilePaused();
    try {
      verifyCluster();
    } catch (RuntimeException e) {
      admin.close();
      throw e;
    }
    LOG.info("Kafka ACL authorization plugin created for metalake {}: {}", metalake, properties);
  }

  /**
   * Returns the state of the plugin.
   *
   * @return {@link Health#PAUSED} while reconcile is paused, {@link Health#HEALTHY} otherwise
   */
  public Health health() {
    return paused ? Health.PAUSED : Health.HEALTHY;
  }

  /**
   * Brings the cluster's owned ACLs in line with the effective grants.
   *
   * @param pending entities being removed by the calling hook
   * @param deleteFirst delete stale ACLs before creating new ones (used for revokes, so access is
   *     never briefly widened)
   * @return {@link Boolean#TRUE} on success
   * @throws AuthorizationPluginException if a grant cannot be translated or Kafka fails
   */
  @VisibleForTesting
  synchronized Boolean reconcile(PendingRemovals pending, boolean deleteFirst) {
    Set<AclBinding> desired =
        translator.translate(grantSource.grants(pending)).stream()
            .filter(this::owned)
            .collect(Collectors.toSet());
    Set<AclBinding> actual =
        get(admin.describeAcls(AclBindingFilter.ANY).values(), "describe").stream()
            .filter(this::owned)
            .collect(Collectors.toSet());

    Set<AclBinding> toCreate = Sets.difference(desired, actual).immutableCopy();
    Set<AclBinding> toDelete = Sets.difference(actual, desired).immutableCopy();
    if (deleteFirst) {
      delete(toDelete);
      create(toCreate);
    } else {
      create(toCreate);
      delete(toDelete);
    }
    LOG.info(
        "Reconciled Kafka ACLs: {} desired, {} created, {} deleted",
        desired.size(),
        toCreate.size(),
        toDelete.size());
    return Boolean.TRUE;
  }

  @Override
  public Boolean onMetadataUpdated(MetadataObjectChange... changes) throws RuntimeException {
    PendingRemovals pending = PendingRemovals.none();
    Set<String> removedTopics = Sets.newHashSet();
    for (MetadataObjectChange change : changes) {
      if (change instanceof MetadataObjectChange.RemoveMetadataObject) {
        MetadataObject object =
            ((MetadataObjectChange.RemoveMetadataObject) change).metadataObject();
        if (object.type() == MetadataObject.Type.CATALOG) {
          pending = PendingRemovals.catalog();
        } else if (object.type() == MetadataObject.Type.TOPIC) {
          removedTopics.add(object.fullName());
        }
      }
    }
    if (!pending.removesCatalog() && !removedTopics.isEmpty()) {
      pending = PendingRemovals.topics(removedTopics);
    }
    PendingRemovals removals = pending;
    return hook("onMetadataUpdated", () -> reconcile(removals, true));
  }

  @Override
  public Boolean onRoleCreated(Role role) throws AuthorizationPluginException {
    return hook("onRoleCreated", () -> reconcile(PendingRemovals.none(), false));
  }

  @Override
  public Boolean onRoleAcquired(Role role) throws AuthorizationPluginException {
    return hook("onRoleAcquired", () -> reconcile(PendingRemovals.none(), false));
  }

  @Override
  public Boolean onRoleDeleted(Role role) throws AuthorizationPluginException {
    // Core calls this hook before deleting the role from the store.
    return hook("onRoleDeleted", () -> reconcile(PendingRemovals.role(role.name()), true));
  }

  @Override
  public Boolean onRoleUpdated(Role role, RoleChange... changes)
      throws AuthorizationPluginException {
    return hook("onRoleUpdated", () -> reconcile(PendingRemovals.none(), true));
  }

  @Override
  public Boolean onGrantedRolesToUser(List<Role> roles, User user)
      throws AuthorizationPluginException {
    return hook("onGrantedRolesToUser", () -> reconcile(PendingRemovals.none(), false));
  }

  @Override
  public Boolean onRevokedRolesFromUser(List<Role> roles, User user)
      throws AuthorizationPluginException {
    return hook("onRevokedRolesFromUser", () -> reconcile(PendingRemovals.none(), true));
  }

  @Override
  public Boolean onGrantedRolesToGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException {
    return hook("onGrantedRolesToGroup", () -> reconcile(PendingRemovals.none(), false));
  }

  @Override
  public Boolean onRevokedRolesFromGroup(List<Role> roles, Group group)
      throws AuthorizationPluginException {
    return hook("onRevokedRolesFromGroup", () -> reconcile(PendingRemovals.none(), true));
  }

  @Override
  public Boolean onUserAdded(User user) throws AuthorizationPluginException {
    return hook("onUserAdded", () -> reconcile(PendingRemovals.none(), false));
  }

  @Override
  public Boolean onUserRemoved(User user) throws AuthorizationPluginException {
    return hook("onUserRemoved", () -> reconcile(PendingRemovals.user(user.name()), true));
  }

  @Override
  public Boolean onUserAcquired(User user) throws AuthorizationPluginException {
    return hook("onUserAcquired", () -> reconcile(PendingRemovals.none(), false));
  }

  @Override
  public Boolean onGroupAdded(Group group) throws AuthorizationPluginException {
    return hook("onGroupAdded", () -> reconcile(PendingRemovals.none(), false));
  }

  @Override
  public Boolean onGroupRemoved(Group group) throws AuthorizationPluginException {
    return hook("onGroupRemoved", () -> reconcile(PendingRemovals.group(group.name()), true));
  }

  @Override
  public Boolean onGroupAcquired(Group group) throws AuthorizationPluginException {
    return hook("onGroupAcquired", () -> reconcile(PendingRemovals.none(), false));
  }

  @Override
  public Boolean onOwnerSet(MetadataObject metadataObject, Owner preOwner, Owner newOwner)
      throws AuthorizationPluginException {
    return hook("onOwnerSet", () -> reconcile(PendingRemovals.none(), false));
  }

  @Override
  public void close() throws IOException {
    admin.close();
  }

  private Boolean hook(String name, Supplier<Boolean> reconcile) {
    if (paused) {
      LOG.warn("kafka-acl reconcile paused; skipped {}", name);
      return Boolean.TRUE;
    }
    return reconcile.get();
  }

  private boolean owned(AclBinding binding) {
    return resolver.owns(binding.entry().principal());
  }

  private void create(Set<AclBinding> bindings) {
    if (!bindings.isEmpty()) {
      LOG.info("Creating Kafka ACLs: {}", bindings);
      get(admin.createAcls(bindings).all(), "create");
    }
  }

  private void delete(Set<AclBinding> bindings) {
    if (!bindings.isEmpty()) {
      LOG.info("Deleting Kafka ACLs: {}", bindings);
      List<AclBindingFilter> filters =
          bindings.stream().map(AclBinding::toFilter).collect(Collectors.toList());
      get(admin.deleteAcls(filters).all(), "delete");
    }
  }

  private void verifyCluster() {
    try {
      admin.describeCluster().clusterId().get();
    } catch (ExecutionException e) {
      throw new AuthorizationPluginException(
          e.getCause(),
          "Cannot reach the Kafka cluster at %s: %s",
          properties.adminClientConfig().get(KafkaAclProperties.BOOTSTRAP_SERVERS),
          e.getCause().getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AuthorizationPluginException(e, "Interrupted while connecting to Kafka");
    }
    try {
      admin.describeAcls(AclBindingFilter.ANY).values().get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SecurityDisabledException) {
        throw new AuthorizationPluginException(
            e.getCause(),
            "Kafka authorization is not enabled on the cluster at %s; configure an authorizer"
                + " (e.g. StandardAuthorizer) before using the %s plugin: %s",
            properties.adminClientConfig().get(KafkaAclProperties.BOOTSTRAP_SERVERS),
            KafkaAclAuthorization.SHORT_NAME,
            e.getCause().getMessage());
      }
      throw new AuthorizationPluginException(
          e.getCause(), "Failed to describe Kafka ACLs: %s", e.getCause().getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AuthorizationPluginException(e, "Interrupted while describing Kafka ACLs");
    }
  }

  private static <T> T get(KafkaFuture<T> future, String operation) {
    try {
      return future.get();
    } catch (ExecutionException e) {
      throw new AuthorizationPluginException(
          e.getCause(), "Failed to %s Kafka ACLs: %s", operation, e.getCause().getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AuthorizationPluginException(
          e, "Interrupted while trying to %s Kafka ACLs", operation);
    }
  }

  private static KafkaAclProperties validatedProperties(Map<String, String> config) {
    KafkaAclProperties properties = new KafkaAclProperties(config);
    properties.validate();
    return properties;
  }

  private static Admin createAdmin(Map<String, String> config) {
    return Admin.create(validatedProperties(config).adminClientConfig());
  }

  private static EffectiveGrantSource gravitinoGrantSource(
      String metalake, Map<String, String> config) {
    String catalogId = config.get(BaseAuthorization.CATALOG_ID);
    Preconditions.checkArgument(catalogId != null, "Catalog ID is required");
    return new GravitinoEffectiveGrantSource(
        metalake,
        Long.parseLong(catalogId),
        KafkaPrincipalResolver.withoutGroupMembership(
            validatedProperties(config).principalPrefix()));
  }
}
