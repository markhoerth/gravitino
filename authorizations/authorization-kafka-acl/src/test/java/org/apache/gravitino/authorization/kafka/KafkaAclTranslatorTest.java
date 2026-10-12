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
import static org.apache.gravitino.authorization.Privilege.Name.CREATE_TOPIC;
import static org.apache.gravitino.authorization.Privilege.Name.PRODUCE_TOPIC;
import static org.apache.gravitino.authorization.kafka.KafkaGrant.Effect.ALLOW;
import static org.apache.gravitino.authorization.kafka.KafkaGrant.Effect.DENY;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class KafkaAclTranslatorTest {

  private static final String P = "User:flink.order_enricher";
  private static final String TOPIC = "order_events";

  private final KafkaAclTranslator translator =
      new KafkaAclTranslator("analytics", KafkaAclProperties.DEFAULT_GROUP_PREFIX_TEMPLATE);

  @Test
  public void testConsumeAllow() {
    Set<AclBinding> bindings =
        translator.translate(ImmutableList.of(KafkaGrant.topic(P, TOPIC, CONSUME_TOPIC, ALLOW)));

    Assertions.assertEquals(
        ImmutableSet.of(
            topic(AclOperation.READ, AclPermissionType.ALLOW),
            topic(AclOperation.DESCRIBE, AclPermissionType.ALLOW),
            group(P, "flink.analytics.flink.order_enricher.")),
        bindings);
  }

  @Test
  public void testConsumeDeny() {
    Set<AclBinding> bindings =
        translator.translate(ImmutableList.of(KafkaGrant.topic(P, TOPIC, CONSUME_TOPIC, DENY)));

    Assertions.assertEquals(
        ImmutableSet.of(
            topic(AclOperation.READ, AclPermissionType.DENY),
            topic(AclOperation.DESCRIBE, AclPermissionType.DENY)),
        bindings);
  }

  @Test
  public void testProduceAllow() {
    Set<AclBinding> bindings =
        translator.translate(ImmutableList.of(KafkaGrant.topic(P, TOPIC, PRODUCE_TOPIC, ALLOW)));

    Assertions.assertEquals(
        ImmutableSet.of(
            topic(AclOperation.WRITE, AclPermissionType.ALLOW),
            topic(AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
        bindings);
  }

  @Test
  public void testProduceDeny() {
    Set<AclBinding> bindings =
        translator.translate(ImmutableList.of(KafkaGrant.topic(P, TOPIC, PRODUCE_TOPIC, DENY)));

    Assertions.assertEquals(
        ImmutableSet.of(topic(AclOperation.WRITE, AclPermissionType.DENY)), bindings);
  }

  @Test
  public void testDenyWinsOverAllowAcrossRoles() {
    // Role A grants CONSUME_TOPIC, role B denies it; both are held by the same Principal.
    Set<AclBinding> bindings =
        translator.translate(
            ImmutableList.of(
                KafkaGrant.topic(P, TOPIC, CONSUME_TOPIC, ALLOW),
                KafkaGrant.topic(P, TOPIC, CONSUME_TOPIC, DENY)));

    Assertions.assertEquals(
        ImmutableSet.of(
            topic(AclOperation.READ, AclPermissionType.DENY),
            topic(AclOperation.DESCRIBE, AclPermissionType.DENY)),
        bindings);
    Assertions.assertTrue(
        bindings.stream().allMatch(b -> b.entry().permissionType() == AclPermissionType.DENY));
  }

  @Test
  public void testGroupPrefixFromTemplate() {
    KafkaAclTranslator custom = new KafkaAclTranslator("sales", "app-${principal}-${metalake}-");
    Assertions.assertEquals("app-flink.x-sales-", custom.groupPrefix("User:flink.x"));

    Set<AclBinding> bindings =
        custom.translate(
            ImmutableList.of(
                KafkaGrant.topic("User:flink.x", "a", CONSUME_TOPIC, ALLOW),
                KafkaGrant.topic("User:flink.x", "b", CONSUME_TOPIC, ALLOW),
                KafkaGrant.topic("User:flink.y", "a", CONSUME_TOPIC, ALLOW)));
    Assertions.assertEquals(
        ImmutableSet.of(
            group("User:flink.x", "app-flink.x-sales-"),
            group("User:flink.y", "app-flink.y-sales-")),
        groups(bindings));
  }

  @ParameterizedTest
  @EnumSource(
      value = KafkaGrant.Scope.class,
      names = {"CATALOG", "SCHEMA", "METALAKE"})
  public void testNonTopicScopeRejected(KafkaGrant.Scope scope) {
    for (Privilege.Name privilege : ImmutableList.of(CONSUME_TOPIC, PRODUCE_TOPIC)) {
      AuthorizationPluginException e =
          Assertions.assertThrows(
              AuthorizationPluginException.class,
              () ->
                  translator.translate(
                      ImmutableList.of(
                          new KafkaGrant(P, scope, "kafka_catalog", privilege, ALLOW))));
      Assertions.assertTrue(
          e.getMessage()
              .contains(
                  "catalog-level Kafka grants need an explicit wildcard or prefix mapping;"
                      + " not supported yet"),
          e.getMessage());
    }
  }

  @Test
  public void testCreateTopicSkipped() {
    Assertions.assertTrue(
        translator
            .translate(
                ImmutableList.of(
                    KafkaGrant.topic(P, TOPIC, CREATE_TOPIC, ALLOW),
                    new KafkaGrant(
                        P, KafkaGrant.Scope.CATALOG, "kafka_catalog", CREATE_TOPIC, ALLOW)))
            .isEmpty());
  }

  @Test
  public void testNoWildcardEverEmitted() {
    Set<AclBinding> bindings =
        translator.translate(
            ImmutableList.of(
                KafkaGrant.topic(P, TOPIC, CONSUME_TOPIC, ALLOW),
                KafkaGrant.topic(P, "other_topic", PRODUCE_TOPIC, ALLOW),
                KafkaGrant.topic("User:flink.other", TOPIC, CONSUME_TOPIC, DENY),
                KafkaGrant.topic("User:flink.third", "t3", CONSUME_TOPIC, ALLOW)));

    Assertions.assertFalse(bindings.isEmpty());
    for (AclBinding binding : bindings) {
      ResourcePattern pattern = binding.pattern();
      Assertions.assertNotEquals(
          ResourcePattern.WILDCARD_RESOURCE, pattern.name(), binding.toString());
      Assertions.assertFalse(pattern.name().isEmpty(), binding.toString());
      Assertions.assertNotEquals(PatternType.ANY, pattern.patternType(), binding.toString());
      Assertions.assertNotEquals(PatternType.MATCH, pattern.patternType(), binding.toString());
      if (pattern.resourceType() == ResourceType.TOPIC) {
        Assertions.assertEquals(PatternType.LITERAL, pattern.patternType(), binding.toString());
      } else {
        Assertions.assertEquals(ResourceType.GROUP, pattern.resourceType(), binding.toString());
        Assertions.assertEquals(PatternType.PREFIXED, pattern.patternType(), binding.toString());
      }
    }

    // A template that collapses to "*" must be refused, not emitted.
    KafkaAclTranslator bad = new KafkaAclTranslator("m", "${principal}");
    Assertions.assertThrows(
        AuthorizationPluginException.class,
        () ->
            bad.translate(
                ImmutableList.of(KafkaGrant.topic("User:*", TOPIC, CONSUME_TOPIC, ALLOW))));
    Assertions.assertThrows(
        AuthorizationPluginException.class,
        () ->
            translator.translate(ImmutableList.of(KafkaGrant.topic(P, "*", CONSUME_TOPIC, ALLOW))));
  }

  private static AclBinding topic(AclOperation op, AclPermissionType permission) {
    return new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, TOPIC, PatternType.LITERAL),
        new AccessControlEntry(P, "*", op, permission));
  }

  private static AclBinding group(String principal, String prefix) {
    return new AclBinding(
        new ResourcePattern(ResourceType.GROUP, prefix, PatternType.PREFIXED),
        new AccessControlEntry(principal, "*", AclOperation.READ, AclPermissionType.ALLOW));
  }

  private static Set<AclBinding> groups(Set<AclBinding> bindings) {
    return bindings.stream()
        .filter(b -> b.pattern().resourceType() == ResourceType.GROUP)
        .collect(ImmutableSet.toImmutableSet());
  }
}
