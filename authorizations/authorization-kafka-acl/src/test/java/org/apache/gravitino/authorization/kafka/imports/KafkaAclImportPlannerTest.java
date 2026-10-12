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
package org.apache.gravitino.authorization.kafka.imports;

import static org.apache.gravitino.authorization.kafka.imports.UnmappedAcl.Reason;
import static org.apache.kafka.common.acl.AclOperation.ALL;
import static org.apache.kafka.common.acl.AclOperation.ALTER_CONFIGS;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.kafka.KafkaAclProperties;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KafkaAclImportPlannerTest {

  static final String P = "User:flink.order_enricher";
  static final String METALAKE = "analytics";
  static final String GROUP_PREFIX = "flink.analytics.flink.order_enricher.";
  static final Set<String> TOPICS = ImmutableSet.of("order_events", "payments", "other_topic");
  static final KafkaAclProperties PROPS =
      new KafkaAclProperties(ImmutableMap.of(KafkaAclProperties.BOOTSTRAP_SERVERS, "k:9092"));

  @Test
  public void testRuleA_NonUserPrincipal() {
    assertUnmapped(
        topic("Group:flink.team", "order_events", READ, ALLOW), Reason.NON_USER_PRINCIPAL);
    assertUnmapped(topic("flink.noType", "order_events", READ, ALLOW), Reason.NON_USER_PRINCIPAL);
  }

  @Test
  public void testRuleB_OutsidePrincipalPrefix() {
    assertUnmapped(
        topic("User:alice", "order_events", READ, ALLOW), Reason.OUTSIDE_PRINCIPAL_PREFIX);
    assertUnmapped(topic("User:*", "order_events", READ, ALLOW), Reason.OUTSIDE_PRINCIPAL_PREFIX);
  }

  @Test
  public void testRuleC_HostRestricted() {
    AclBinding acl =
        new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "order_events", PatternType.LITERAL),
            new AccessControlEntry(P, "10.0.0.7", READ, ALLOW));
    assertUnmapped(acl, Reason.HOST_RESTRICTED);
  }

  @Test
  public void testRuleD_OwnGroupPrefixImpliedForeignGroupRejected() {
    AclBinding read = topic(P, "order_events", READ, ALLOW);
    AclBinding ownGroup = group(P, GROUP_PREFIX, PatternType.PREFIXED, READ, ALLOW);
    AclBinding foreignPrefix = group(P, "finance.", PatternType.PREFIXED, READ, ALLOW);
    AclBinding literalGroup = group(P, GROUP_PREFIX, PatternType.LITERAL, READ, ALLOW);
    AclBinding wrongOp = group(P, GROUP_PREFIX, PatternType.PREFIXED, DESCRIBE, ALLOW);
    AclBinding otherPrincipalsGroup =
        group(P, "flink.analytics.flink.other.", PatternType.PREFIXED, READ, ALLOW);

    ImportPlan plan =
        plan(read, ownGroup, foreignPrefix, literalGroup, wrongOp, otherPrincipalsGroup);
    Assertions.assertEquals(ImmutableList.of(ownGroup), plan.implied());
    Assertions.assertEquals(
        ImmutableSet.of(
            new UnmappedAcl(foreignPrefix, Reason.GROUP_NOT_PRINCIPAL_PREFIX),
            new UnmappedAcl(literalGroup, Reason.GROUP_NOT_PRINCIPAL_PREFIX),
            new UnmappedAcl(wrongOp, Reason.GROUP_NOT_PRINCIPAL_PREFIX),
            new UnmappedAcl(otherPrincipalsGroup, Reason.GROUP_NOT_PRINCIPAL_PREFIX)),
        ImmutableSet.copyOf(plan.unmapped()));
  }

  @Test
  public void testRuleD_OwnGroupWithoutConsumeGrantNotImplied() {
    // The plugin only emits the group READ for a Principal with an effective consume ALLOW.
    AclBinding ownGroup = group(P, GROUP_PREFIX, PatternType.PREFIXED, READ, ALLOW);
    assertUnmapped(ownGroup, Reason.NOT_IMPLIED_BY_GRANT);
  }

  @Test
  public void testRuleE_UnsupportedResourceType() {
    assertUnmapped(
        new AclBinding(
            new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL),
            new AccessControlEntry(P, "*", AclOperation.IDEMPOTENT_WRITE, ALLOW)),
        Reason.UNSUPPORTED_RESOURCE_TYPE);
    assertUnmapped(
        new AclBinding(
            new ResourcePattern(ResourceType.TRANSACTIONAL_ID, "tx-1", PatternType.LITERAL),
            new AccessControlEntry(P, "*", WRITE, ALLOW)),
        Reason.UNSUPPORTED_RESOURCE_TYPE);
  }

  @Test
  public void testRuleF_NonLiteralTopicPattern() {
    assertUnmapped(
        new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "order_", PatternType.PREFIXED),
            new AccessControlEntry(P, "*", READ, ALLOW)),
        Reason.NON_LITERAL_TOPIC_PATTERN);
    // LITERAL "*" is Kafka's wildcard topic.
    assertUnmapped(topic(P, "*", READ, ALLOW), Reason.NON_LITERAL_TOPIC_PATTERN);
  }

  @Test
  public void testRuleG_TopicNotInCatalog() {
    assertUnmapped(topic(P, "not_in_catalog", READ, ALLOW), Reason.TOPIC_NOT_IN_CATALOG);
  }

  @Test
  public void testRuleH_ReadWriteAllowDenyMapped() {
    AclBinding readAllow = topic(P, "order_events", READ, ALLOW);
    AclBinding readDeny = topic(P, "payments", READ, DENY);
    AclBinding writeAllow = topic(P, "other_topic", WRITE, ALLOW);
    AclBinding writeDeny = topic(P, "order_events", WRITE, DENY);

    ImportPlan plan = plan(readAllow, readDeny, writeAllow, writeDeny);
    Assertions.assertTrue(plan.unmapped().isEmpty(), plan.unmapped().toString());
    Assertions.assertEquals(
        ImmutableSet.of(
            new MappedGrant(
                "flink.order_enricher",
                "order_events",
                Privilege.Name.CONSUME_TOPIC,
                false,
                readAllow),
            new MappedGrant(
                "flink.order_enricher", "payments", Privilege.Name.CONSUME_TOPIC, true, readDeny),
            new MappedGrant(
                "flink.order_enricher",
                "other_topic",
                Privilege.Name.PRODUCE_TOPIC,
                false,
                writeAllow),
            new MappedGrant(
                "flink.order_enricher",
                "order_events",
                Privilege.Name.PRODUCE_TOPIC,
                true,
                writeDeny)),
        ImmutableSet.copyOf(plan.mapped()));
  }

  @Test
  public void testRuleH_DescribeImpliedVsDescribeOnly() {
    AclBinding read = topic(P, "order_events", READ, ALLOW);
    AclBinding describeWithRead = topic(P, "order_events", DESCRIBE, ALLOW);
    AclBinding describeAlone = topic(P, "payments", DESCRIBE, ALLOW);
    AclBinding write = topic(P, "other_topic", WRITE, ALLOW);
    AclBinding describeWithWrite = topic(P, "other_topic", DESCRIBE, ALLOW);

    ImportPlan plan = plan(read, describeWithRead, describeAlone, write, describeWithWrite);
    Assertions.assertTrue(
        plan.implied().containsAll(ImmutableList.of(describeWithRead, describeWithWrite)));
    Assertions.assertEquals(
        ImmutableList.of(new UnmappedAcl(describeAlone, Reason.DESCRIBE_ONLY)), plan.unmapped());
  }

  @Test
  public void testRuleH_DescribeNotRegeneratedIsNotImplied() {
    // READ ALLOW is overridden by READ DENY, so the plugin emits DESCRIBE DENY, not ALLOW.
    AclBinding readAllow = topic(P, "order_events", READ, ALLOW);
    AclBinding readDeny = topic(P, "order_events", READ, DENY);
    AclBinding describeAllow = topic(P, "order_events", DESCRIBE, ALLOW);

    ImportPlan plan = plan(readAllow, readDeny, describeAllow);
    Assertions.assertEquals(
        ImmutableSet.of(
            new UnmappedAcl(readAllow, Reason.SHADOWED_BY_DENY),
            new UnmappedAcl(describeAllow, Reason.NOT_IMPLIED_BY_GRANT)),
        ImmutableSet.copyOf(plan.unmapped()));
    Assertions.assertEquals(1, plan.mapped().size());
    Assertions.assertTrue(plan.mapped().get(0).deny());
    Assertions.assertEquals(
        ImmutableList.of(topic(P, "order_events", DESCRIBE, DENY)), plan.additions());
  }

  @Test
  public void testRuleH_UnsupportedOperation() {
    assertUnmapped(topic(P, "order_events", ALL, ALLOW), Reason.UNSUPPORTED_OPERATION);
    assertUnmapped(
        topic(P, "order_events", AclOperation.DELETE, ALLOW), Reason.UNSUPPORTED_OPERATION);
    assertUnmapped(topic(P, "order_events", ALTER_CONFIGS, ALLOW), Reason.UNSUPPORTED_OPERATION);
  }

  @Test
  public void testRulesAppliedInOrder() {
    // Outside the prefix wins over host restriction and resource type.
    AclBinding acl =
        new AclBinding(
            new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL),
            new AccessControlEntry("User:alice", "10.0.0.1", ALL, ALLOW));
    assertUnmapped(acl, Reason.OUTSIDE_PRINCIPAL_PREFIX);
    // Host restriction wins over group and topic checks.
    assertUnmapped(
        new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "order_", PatternType.PREFIXED),
            new AccessControlEntry(P, "10.0.0.1", READ, ALLOW)),
        Reason.HOST_RESTRICTED);
  }

  @Test
  public void testDeterministicOrdering() {
    List<AclBinding> acls =
        new ArrayList<>(
            ImmutableList.of(
                topic("User:flink.b", "payments", READ, ALLOW),
                topic("User:flink.a", "payments", WRITE, ALLOW),
                topic("User:flink.a", "order_events", READ, ALLOW),
                topic("User:flink.a", "order_events", WRITE, ALLOW),
                topic("User:flink.a", "order_events", DESCRIBE, ALLOW),
                topic("User:flink.b", "payments", DESCRIBE, ALLOW),
                topic("User:zed", "x", READ, ALLOW),
                topic("User:alice", "x", READ, ALLOW),
                topic("User:flink.a", "zzz", READ, ALLOW),
                topic("User:flink.a", "aaa", READ, ALLOW)));
    ImportPlan first = plan(acls);
    for (int i = 0; i < 5; i++) {
      Collections.shuffle(acls);
      ImportPlan again = plan(acls);
      Assertions.assertEquals(first.mapped(), again.mapped());
      Assertions.assertEquals(first.unmapped(), again.unmapped());
      Assertions.assertEquals(first.implied(), again.implied());
      Assertions.assertEquals(first.additions(), again.additions());
    }
    Assertions.assertEquals(
        ImmutableList.of(
            "flink.a/order_events/READ",
            "flink.a/order_events/WRITE",
            "flink.a/payments/WRITE",
            "flink.b/payments/READ"),
        first.mapped().stream()
            .map(g -> g.gravitinoUser() + "/" + g.topic() + "/" + g.source().entry().operation())
            .collect(ImmutableList.toImmutableList()));
    Assertions.assertEquals(
        ImmutableList.of("User:alice", "User:flink.a", "User:flink.a", "User:zed"),
        first.unmapped().stream()
            .map(u -> u.acl().entry().principal())
            .collect(ImmutableList.toImmutableList()));
  }

  static ImportPlan plan(AclBinding... acls) {
    return plan(ImmutableList.copyOf(acls));
  }

  static ImportPlan plan(List<AclBinding> acls) {
    return KafkaAclImportPlanner.plan(acls, PROPS, METALAKE, TOPICS);
  }

  private static void assertUnmapped(AclBinding acl, Reason reason) {
    ImportPlan plan = plan(acl);
    Assertions.assertEquals(ImmutableList.of(new UnmappedAcl(acl, reason)), plan.unmapped());
    Assertions.assertTrue(plan.mapped().isEmpty());
    Assertions.assertTrue(plan.implied().isEmpty());
  }

  static AclBinding topic(String principal, String topic, AclOperation op, AclPermissionType perm) {
    return new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
        new AccessControlEntry(principal, "*", op, perm));
  }

  static AclBinding group(
      String principal, String name, PatternType type, AclOperation op, AclPermissionType perm) {
    return new AclBinding(
        new ResourcePattern(ResourceType.GROUP, name, type),
        new AccessControlEntry(principal, "*", op, perm));
  }
}
