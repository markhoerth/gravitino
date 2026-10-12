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

import static org.apache.gravitino.authorization.Privilege.Name.CONSUME_TOPIC;
import static org.apache.gravitino.authorization.kafka.KafkaGrant.Effect.ALLOW;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KafkaAclAuthorizationPluginTest {

  private static final String P = "User:flink.order_enricher";
  private static final Map<String, String> CONFIG =
      ImmutableMap.of(KafkaAclProperties.BOOTSTRAP_SERVERS, "localhost:9092");

  private final List<KafkaGrant> grants = new CopyOnWriteArrayList<>();
  private final EffectiveGrantSource source = pending -> KafkaGrant.effective(grants);

  /** The ACLs the fake cluster holds; updated by the mocked create/delete calls. */
  private final Set<AclBinding> cluster = new HashSet<>();

  private final List<Collection<AclBinding>> creates = new ArrayList<>();
  private final List<Collection<AclBindingFilter>> deletes = new ArrayList<>();
  private Admin admin;

  @BeforeEach
  public void setUp() {
    admin = mock(Admin.class);
    DescribeClusterResult cluster = mock(DescribeClusterResult.class);
    when(cluster.clusterId()).thenReturn(KafkaFuture.completedFuture("test-cluster"));
    when(admin.describeCluster()).thenReturn(cluster);

    when(admin.describeAcls(any(AclBindingFilter.class)))
        .thenAnswer(
            inv -> {
              DescribeAclsResult result = mock(DescribeAclsResult.class);
              when(result.values())
                  .thenReturn(KafkaFuture.completedFuture(ImmutableList.copyOf(this.cluster)));
              return result;
            });
    when(admin.createAcls(anyCollection()))
        .thenAnswer(
            inv -> {
              Collection<AclBinding> bindings = inv.getArgument(0);
              creates.add(ImmutableList.copyOf(bindings));
              this.cluster.addAll(bindings);
              CreateAclsResult result = mock(CreateAclsResult.class);
              when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
              return result;
            });
    when(admin.deleteAcls(anyCollection()))
        .thenAnswer(
            inv -> {
              Collection<AclBindingFilter> filters = inv.getArgument(0);
              deletes.add(ImmutableList.copyOf(filters));
              this.cluster.removeIf(b -> filters.stream().anyMatch(f -> f.matches(b)));
              DeleteAclsResult result = mock(DeleteAclsResult.class);
              when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
              return result;
            });
  }

  @Test
  public void testGrantCreatesBindings() {
    KafkaAclAuthorizationPlugin plugin = newPlugin();
    grants.add(KafkaGrant.topic(P, "order_events", CONSUME_TOPIC, ALLOW));

    Assertions.assertEquals(
        Boolean.TRUE,
        plugin.onGrantedRolesToUser(ImmutableList.of(mock(Role.class)), mock(User.class)));

    Set<AclBinding> expected =
        ImmutableSet.of(
            topic("order_events", AclOperation.READ),
            topic("order_events", AclOperation.DESCRIBE),
            new AclBinding(
                new ResourcePattern(
                    ResourceType.GROUP,
                    "flink.analytics.flink.order_enricher.",
                    PatternType.PREFIXED),
                new AccessControlEntry(P, "*", AclOperation.READ, AclPermissionType.ALLOW)));
    Assertions.assertEquals(1, creates.size());
    Assertions.assertEquals(expected, ImmutableSet.copyOf(creates.get(0)));
    Assertions.assertTrue(deletes.isEmpty());
    Assertions.assertEquals(expected, cluster);
  }

  @Test
  public void testRevokeDeletesBindings() {
    KafkaAclAuthorizationPlugin plugin = newPlugin();
    grants.add(KafkaGrant.topic(P, "order_events", CONSUME_TOPIC, ALLOW));
    plugin.onGrantedRolesToUser(ImmutableList.of(), mock(User.class));
    Set<AclBinding> granted = ImmutableSet.copyOf(cluster);

    grants.clear();
    Assertions.assertEquals(
        Boolean.TRUE, plugin.onRevokedRolesFromUser(ImmutableList.of(), mock(User.class)));

    Assertions.assertEquals(1, deletes.size());
    Assertions.assertEquals(
        granted.stream().map(AclBinding::toFilter).collect(Collectors.toSet()),
        ImmutableSet.copyOf(deletes.get(0)));
    Assertions.assertTrue(cluster.isEmpty());
  }

  @Test
  public void testRoleDeletedExcludesRoleStillInStore() {
    // Core calls onRoleDeleted before removing the role, so the source must be told to skip it.
    List<PendingRemovals> seen = new ArrayList<>();
    EffectiveGrantSource recording =
        pending -> {
          seen.add(pending);
          return pending.removesRole("readers")
              ? ImmutableList.of()
              : ImmutableList.of(KafkaGrant.topic(P, "order_events", CONSUME_TOPIC, ALLOW));
        };
    KafkaAclAuthorizationPlugin plugin =
        new KafkaAclAuthorizationPlugin("analytics", CONFIG, recording, admin);
    plugin.onRoleCreated(mock(Role.class));
    Assertions.assertFalse(cluster.isEmpty());

    Role role = mock(Role.class);
    when(role.name()).thenReturn("readers");
    plugin.onRoleDeleted(role);
    Assertions.assertTrue(seen.get(seen.size() - 1).removesRole("readers"));
    Assertions.assertTrue(cluster.isEmpty());
  }

  @Test
  public void testReconcileIsIdempotent() {
    KafkaAclAuthorizationPlugin plugin = newPlugin();
    grants.add(KafkaGrant.topic(P, "order_events", CONSUME_TOPIC, ALLOW));
    plugin.reconcile(PendingRemovals.none(), false);
    int createCalls = creates.size();
    int deleteCalls = deletes.size();

    Assertions.assertEquals(Boolean.TRUE, plugin.reconcile(PendingRemovals.none(), false));
    Assertions.assertEquals(Boolean.TRUE, plugin.reconcile(PendingRemovals.none(), true));

    Assertions.assertEquals(createCalls, creates.size());
    Assertions.assertEquals(deleteCalls, deletes.size());
    verify(admin, never()).deleteAcls(anyCollection());
  }

  @Test
  public void testBindingOutsidePrefixNeverDeleted() {
    AclBinding alice =
        new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "order_events", PatternType.LITERAL),
            new AccessControlEntry("User:alice", "*", AclOperation.READ, AclPermissionType.ALLOW));
    cluster.add(alice);
    KafkaAclAuthorizationPlugin plugin = newPlugin();

    grants.add(KafkaGrant.topic(P, "order_events", CONSUME_TOPIC, ALLOW));
    plugin.onGrantedRolesToUser(ImmutableList.of(), mock(User.class));
    grants.clear();
    plugin.onRevokedRolesFromUser(ImmutableList.of(), mock(User.class));
    // A grant source that wrongly reports alice must not make the plugin manage her ACLs either.
    grants.add(KafkaGrant.topic("User:alice", "other_topic", CONSUME_TOPIC, ALLOW));
    plugin.onRoleUpdated(mock(Role.class));

    Assertions.assertEquals(ImmutableSet.of(alice), cluster);
    for (Collection<AclBindingFilter> batch : deletes) {
      Assertions.assertTrue(batch.stream().noneMatch(f -> f.matches(alice)), batch.toString());
    }
  }

  @Test
  public void testKafkaErrorWrapped() {
    KafkaAclAuthorizationPlugin plugin = newPlugin();
    KafkaFutureImpl<Void> failed = new KafkaFutureImpl<>();
    ClusterAuthorizationException cause = new ClusterAuthorizationException("not allowed to alter");
    failed.completeExceptionally(cause);
    CreateAclsResult result = mock(CreateAclsResult.class);
    when(result.all()).thenReturn(failed);
    when(admin.createAcls(anyCollection())).thenReturn(result);
    grants.add(KafkaGrant.topic(P, "order_events", CONSUME_TOPIC, ALLOW));

    AuthorizationPluginException e =
        Assertions.assertThrows(
            AuthorizationPluginException.class,
            () -> plugin.onGrantedRolesToUser(ImmutableList.of(), mock(User.class)));
    Assertions.assertSame(cause, e.getCause());
    Assertions.assertTrue(e.getMessage().contains("not allowed to alter"), e.getMessage());
  }

  @Test
  public void testInitFailsWhenAuthorizerDisabled() {
    KafkaFutureImpl<Collection<AclBinding>> failed = new KafkaFutureImpl<>();
    failed.completeExceptionally(new SecurityDisabledException("No Authorizer is configured."));
    DescribeAclsResult result = mock(DescribeAclsResult.class);
    when(result.values()).thenReturn(failed);
    when(admin.describeAcls(any(AclBindingFilter.class))).thenReturn(result);

    AuthorizationPluginException e =
        Assertions.assertThrows(AuthorizationPluginException.class, this::newPlugin);
    Assertions.assertTrue(
        e.getMessage().contains("Kafka authorization is not enabled"), e.getMessage());
    verify(admin).close();
  }

  private KafkaAclAuthorizationPlugin newPlugin() {
    return new KafkaAclAuthorizationPlugin("analytics", CONFIG, source, admin);
  }

  private static AclBinding topic(String topic, AclOperation op) {
    return new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
        new AccessControlEntry(P, "*", op, AclPermissionType.ALLOW));
  }
}
