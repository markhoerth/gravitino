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

import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.authorization.kafka.imports.ImportPlan;
import org.apache.gravitino.authorization.kafka.imports.KafkaAclImportPlanner;
import org.apache.gravitino.authorization.kafka.imports.MappedGrant;
import org.apache.gravitino.authorization.kafka.imports.UnmappedAcl;
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
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Proves the importer's contract with the plugin: after an import, reconcile deletes none of the
 * mapped or implied ACLs, and the ACLs it does delete are exactly the report's unmapped ACLs under
 * the principal prefix.
 */
public class KafkaAclImportRoundTripTest {

  private static final String A = "User:flink.order_enricher";
  private static final String B = "User:flink.billing";
  private static final String METALAKE = "analytics";
  private static final Set<String> TOPICS =
      ImmutableSet.of("order_events", "payments", "invoices", "audit");
  private static final KafkaAclProperties PROPS =
      new KafkaAclProperties(ImmutableMap.of(KafkaAclProperties.BOOTSTRAP_SERVERS, "k:9092"));

  private static final List<AclBinding> CLUSTER_ACLS =
      ImmutableList.of(
          // A: consumer of order_events with DESCRIBE and its own group prefix.
          topic(A, "order_events", READ, ALLOW),
          topic(A, "order_events", DESCRIBE, ALLOW),
          group(A, "flink.analytics.flink.order_enricher.", PatternType.PREFIXED, READ),
          // A: producer to invoices, DESCRIBE missing (the plugin will add it).
          topic(A, "invoices", WRITE, ALLOW),
          // A: denied on payments, both READ and DESCRIBE.
          topic(A, "payments", READ, DENY),
          topic(A, "payments", DESCRIBE, DENY),
          // B: producer with DESCRIBE, plus a WRITE DENY elsewhere.
          topic(B, "payments", WRITE, ALLOW),
          topic(B, "payments", DESCRIBE, ALLOW),
          topic(B, "audit", WRITE, DENY),
          // B: READ ALLOW shadowed by READ DENY, and the DESCRIBE next to it.
          topic(B, "audit", READ, ALLOW),
          topic(B, "audit", READ, DENY),
          topic(B, "audit", DESCRIBE, ALLOW),
          // B: group READ with no consume grant to back it.
          group(B, "flink.analytics.flink.billing.", PatternType.PREFIXED, READ),
          // Unmapped under the prefix: the plugin will delete these.
          new AclBinding(
              new ResourcePattern(ResourceType.TOPIC, "order_events", PatternType.LITERAL),
              new AccessControlEntry(A, "10.1.2.3", READ, DENY)),
          new AclBinding(
              new ResourcePattern(ResourceType.TOPIC, "orders_", PatternType.PREFIXED),
              new AccessControlEntry(A, "*", READ, ALLOW)),
          new AclBinding(
              new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL),
              new AccessControlEntry(A, "*", AclOperation.IDEMPOTENT_WRITE, ALLOW)),
          group(A, "finance.", PatternType.PREFIXED, READ),
          topic(A, "not_in_catalog", READ, ALLOW),
          topic(A, "order_events", ALL, ALLOW),
          topic(B, "invoices", DESCRIBE, ALLOW),
          // Outside the prefix: never touched.
          topic("User:alice", "order_events", READ, ALLOW),
          topic("Group:flink.team", "order_events", READ, ALLOW));

  @Test
  public void testTranslatorRegeneratesImportedAndImpliedAcls() {
    ImportPlan plan = plan();
    Set<AclBinding> regenerated =
        new KafkaAclTranslator(METALAKE, PROPS.groupPrefixTemplate())
            .translate(KafkaAclImportPlanner.toKafkaGrants(plan.mapped()));

    Assertions.assertFalse(plan.mapped().isEmpty());
    Assertions.assertFalse(plan.implied().isEmpty());
    for (MappedGrant grant : plan.mapped()) {
      Assertions.assertTrue(
          regenerated.contains(grant.source()), "not regenerated: " + grant.source());
    }
    for (AclBinding implied : plan.implied()) {
      Assertions.assertTrue(regenerated.contains(implied), "not regenerated: " + implied);
    }
  }

  @Test
  public void testReconcileDeletesExactlyUnmappedUnderPrefix() {
    ImportPlan plan = plan();
    Set<AclBinding> cluster = new HashSet<>(CLUSTER_ACLS);
    List<AclBindingFilter> deleted = new ArrayList<>();
    List<AclBinding> created = new ArrayList<>();
    Admin admin = fakeAdmin(cluster, created, deleted);

    List<KafkaGrant> grants = KafkaAclImportPlanner.toKafkaGrants(plan.mapped());
    KafkaAclAuthorizationPlugin plugin =
        new KafkaAclAuthorizationPlugin(
            METALAKE,
            ImmutableMap.of(KafkaAclProperties.BOOTSTRAP_SERVERS, "k:9092"),
            pending -> KafkaGrant.effective(grants),
            admin);
    plugin.reconcile(PendingRemovals.none(), false);

    Set<AclBinding> deletedAcls =
        CLUSTER_ACLS.stream()
            .filter(acl -> deleted.stream().anyMatch(f -> f.matches(acl)))
            .collect(Collectors.toSet());

    Set<AclBinding> keep = new HashSet<>(plan.implied());
    plan.mapped().forEach(g -> keep.add(g.source()));
    Set<AclBinding> wronglyDeleted =
        deletedAcls.stream().filter(keep::contains).collect(Collectors.toSet());
    Assertions.assertEquals(
        ImmutableSet.of(), wronglyDeleted, "imported ACLs deleted by reconcile");

    Set<AclBinding> expectedDeletes =
        plan.unmapped().stream()
            .map(UnmappedAcl::acl)
            .filter(acl -> acl.entry().principal().startsWith(PROPS.principalPrefix()))
            .collect(Collectors.toSet());
    Assertions.assertEquals(expectedDeletes, deletedAcls);
    Assertions.assertEquals(ImmutableSet.copyOf(plan.additions()), ImmutableSet.copyOf(created));

    // Sanity on the fixture: every rule outcome is represented.
    Set<UnmappedAcl.Reason> reasons =
        plan.unmapped().stream().map(UnmappedAcl::reason).collect(Collectors.toSet());
    Assertions.assertEquals(
        ImmutableSet.of(
            UnmappedAcl.Reason.NON_USER_PRINCIPAL,
            UnmappedAcl.Reason.OUTSIDE_PRINCIPAL_PREFIX,
            UnmappedAcl.Reason.HOST_RESTRICTED,
            UnmappedAcl.Reason.GROUP_NOT_PRINCIPAL_PREFIX,
            UnmappedAcl.Reason.UNSUPPORTED_RESOURCE_TYPE,
            UnmappedAcl.Reason.NON_LITERAL_TOPIC_PATTERN,
            UnmappedAcl.Reason.TOPIC_NOT_IN_CATALOG,
            UnmappedAcl.Reason.UNSUPPORTED_OPERATION,
            UnmappedAcl.Reason.DESCRIBE_ONLY,
            UnmappedAcl.Reason.SHADOWED_BY_DENY,
            UnmappedAcl.Reason.NOT_IMPLIED_BY_GRANT),
        reasons);

    // A second reconcile after the import settles is a no-op.
    int deleteCalls = deleted.size();
    int createCalls = created.size();
    plugin.reconcile(PendingRemovals.none(), false);
    Assertions.assertEquals(deleteCalls, deleted.size());
    Assertions.assertEquals(createCalls, created.size());
  }

  private static ImportPlan plan() {
    return KafkaAclImportPlanner.plan(CLUSTER_ACLS, PROPS, METALAKE, TOPICS);
  }

  private static Admin fakeAdmin(
      Set<AclBinding> cluster, List<AclBinding> created, List<AclBindingFilter> deleted) {
    Admin admin = mock(Admin.class);
    DescribeClusterResult describeCluster = mock(DescribeClusterResult.class);
    when(describeCluster.clusterId()).thenReturn(KafkaFuture.completedFuture("c"));
    when(admin.describeCluster()).thenReturn(describeCluster);
    when(admin.describeAcls(any(AclBindingFilter.class)))
        .thenAnswer(
            inv -> {
              DescribeAclsResult result = mock(DescribeAclsResult.class);
              when(result.values())
                  .thenReturn(KafkaFuture.completedFuture(ImmutableList.copyOf(cluster)));
              return result;
            });
    when(admin.createAcls(anyCollection()))
        .thenAnswer(
            inv -> {
              Collection<AclBinding> bindings = inv.getArgument(0);
              created.addAll(bindings);
              cluster.addAll(bindings);
              CreateAclsResult result = mock(CreateAclsResult.class);
              when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
              return result;
            });
    when(admin.deleteAcls(anyCollection()))
        .thenAnswer(
            inv -> {
              Collection<AclBindingFilter> filters = inv.getArgument(0);
              deleted.addAll(filters);
              cluster.removeIf(b -> filters.stream().anyMatch(f -> f.matches(b)));
              DeleteAclsResult result = mock(DeleteAclsResult.class);
              when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
              return result;
            });
    return admin;
  }

  private static AclBinding topic(
      String principal, String topic, AclOperation op, AclPermissionType perm) {
    return new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
        new AccessControlEntry(principal, "*", op, perm));
  }

  private static AclBinding group(
      String principal, String name, PatternType type, AclOperation op) {
    return new AclBinding(
        new ResourcePattern(ResourceType.GROUP, name, type),
        new AccessControlEntry(principal, "*", op, ALLOW));
  }
}
