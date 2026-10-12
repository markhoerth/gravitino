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

import static org.apache.gravitino.authorization.kafka.imports.KafkaAclImportTool.roleName;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.common.acl.AclPermissionType.DENY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.kafka.KafkaAclProperties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeAclsResult;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class KafkaAclImportToolTest {

  private static final String A = "User:flink.order_enricher";
  private static final String B = "User:flink.billing-v2";
  private static final String ROLE_A = roleName("flink.order_enricher");
  private static final String ROLE_B = roleName("flink.billing-v2");
  private static final String PAUSE =
      "setCatalogProperty " + KafkaAclProperties.RECONCILE_PAUSED + "=true";
  private static final String UNPAUSE =
      "setCatalogProperty " + KafkaAclProperties.RECONCILE_PAUSED + "=false";
  private static final List<AclBinding> BASE_ACLS =
      ImmutableList.of(
          topic(A, "order_events", READ, ALLOW),
          topic(A, "order_events", DESCRIBE, ALLOW),
          topic(A, "payments", READ, DENY),
          new AclBinding(
              new ResourcePattern(
                  ResourceType.GROUP,
                  "flink.analytics.flink.order_enricher.",
                  PatternType.PREFIXED),
              new AccessControlEntry(A, "*", READ, ALLOW)),
          topic(B, "invoices", WRITE, ALLOW),
          topic(B, "not_in_catalog", WRITE, ALLOW),
          topic("User:alice", "order_events", READ, ALLOW));

  @TempDir Path tmp;
  private final List<AclBinding> acls = new ArrayList<>(BASE_ACLS);
  private FakeImportTarget target;
  private Admin admin;
  private ByteArrayOutputStream out;

  @BeforeEach
  public void setUp() {
    target =
        new FakeImportTarget(
            ImmutableMap.of(KafkaAclProperties.BOOTSTRAP_SERVERS, "k:9092"),
            ImmutableSet.of("order_events", "payments", "invoices"));
    admin = mock(Admin.class);
    when(admin.describeAcls(any(AclBindingFilter.class)))
        .thenAnswer(
            inv -> {
              DescribeAclsResult result = mock(DescribeAclsResult.class);
              when(result.values())
                  .thenReturn(KafkaFuture.completedFuture(ImmutableList.copyOf(acls)));
              return result;
            });
    out = new ByteArrayOutputStream();
  }

  @Test
  public void testDryRunMakesNoWrites() throws Exception {
    KafkaAclImportTool.Outcome outcome = run(false, false);

    Assertions.assertTrue(outcome.succeeded());
    Assertions.assertEquals(ImmutableList.of(), target.writes);
    // Only the two reads needed to plan; not even existence checks.
    Assertions.assertEquals(
        ImmutableSet.of("listTopics", "catalogProperties"), ImmutableSet.copyOf(target.calls));

    JsonNode report = readReport();
    Assertions.assertEquals("dry-run", report.get("mode").asText());
    Assertions.assertEquals(3, report.at("/summary/mapped").asInt());
    Assertions.assertEquals(2, report.at("/summary/implied").asInt());
    Assertions.assertEquals(2, report.at("/summary/unmapped").asInt());
    Assertions.assertEquals(1, report.at("/summary/unmappedByReason/TOPIC_NOT_IN_CATALOG").asInt());
    Assertions.assertEquals(
        1, report.at("/summary/unmappedByReason/OUTSIDE_PRINCIPAL_PREFIX").asInt());
    Assertions.assertEquals(1, report.at("/summary/pluginWillDelete").asInt());
    for (JsonNode role : report.get("roles")) {
      Assertions.assertEquals("PLANNED", role.get("status").asText());
    }
    Assertions.assertTrue(out.toString(StandardCharsets.UTF_8).contains("mapped (become grants)"));
  }

  @Test
  public void testApplyCreatesUsersRolesAndGrants() throws Exception {
    target.users.add("flink.billing-v2");
    KafkaAclImportTool.Outcome outcome = run(true, false);

    Assertions.assertTrue(outcome.succeeded(), outcome.report().toString());
    Assertions.assertEquals(
        ImmutableList.of(
            PAUSE,
            "createRole " + ROLE_B,
            "grantRoleToUser " + ROLE_B + " flink.billing-v2",
            "addUser flink.order_enricher",
            "createRole " + ROLE_A,
            "grantRoleToUser " + ROLE_A + " flink.order_enricher",
            UNPAUSE,
            "triggerReconcile"),
        target.writes);

    Assertions.assertEquals(
        ImmutableList.of(
            "CATALOG kafka_catalog [ALLOW USE_CATALOG]",
            "SCHEMA kafka_catalog.default [ALLOW USE_SCHEMA]",
            "TOPIC kafka_catalog.default.order_events [ALLOW CONSUME_TOPIC]",
            "TOPIC kafka_catalog.default.payments [DENY CONSUME_TOPIC]"),
        describe(target.roles.get(ROLE_A)));
    Assertions.assertEquals(
        ImmutableList.of(
            "CATALOG kafka_catalog [ALLOW USE_CATALOG]",
            "SCHEMA kafka_catalog.default [ALLOW USE_SCHEMA]",
            "TOPIC kafka_catalog.default.invoices [ALLOW PRODUCE_TOPIC]"),
        describe(target.roles.get(ROLE_B)));

    JsonNode report = readReport();
    Assertions.assertEquals("OK", report.get("status").asText());
    Assertions.assertTrue(report.get("unpaused").asBoolean());
    Assertions.assertEquals("fake", report.get("reconcileTrigger").asText());
    Assertions.assertEquals(0, report.get("missingAfterApply").size());
    for (JsonNode role : report.get("roles")) {
      Assertions.assertEquals("CREATED", role.get("status").asText());
    }
  }

  @Test
  public void testPauseWrapsEveryUserCall() throws Exception {
    run(true, false);

    List<String> calls = target.calls;
    int pause = calls.indexOf(PAUSE);
    int unpause = calls.indexOf(UNPAUSE);
    List<Integer> userCalls = new ArrayList<>();
    for (int i = 0; i < calls.size(); i++) {
      String c = calls.get(i);
      if (c.startsWith("userExists")
          || c.startsWith("addUser")
          || c.startsWith("roleSecurableObjects")
          || c.startsWith("createRole")
          || c.startsWith("grantRoleToUser")) {
        userCalls.add(i);
      }
    }
    Assertions.assertFalse(userCalls.isEmpty());
    Assertions.assertTrue(pause >= 0 && pause < userCalls.get(0), calls.toString());
    Assertions.assertTrue(unpause > userCalls.get(userCalls.size() - 1), calls.toString());
    Assertions.assertEquals(calls.size() - 1, calls.indexOf("triggerReconcile"), calls.toString());
    Assertions.assertEquals(
        "false", target.catalogProperties.get(KafkaAclProperties.RECONCILE_PAUSED));
  }

  @Test
  public void testUnpausesWhenTargetCallThrows() throws Exception {
    target.failOn = "createRole " + ROLE_A;
    KafkaAclImportTool.Outcome outcome = run(true, false);

    Assertions.assertEquals(1, outcome.exitCode());
    Assertions.assertEquals(
        "false", target.catalogProperties.get(KafkaAclProperties.RECONCILE_PAUSED));
    Assertions.assertTrue(
        target.calls.indexOf(UNPAUSE) > target.calls.indexOf("createRole " + ROLE_A));
    Assertions.assertEquals(
        ImmutableMap.of(ROLE_A, "FAILED", ROLE_B, "CREATED"), roleStatuses(readReport()));
    Assertions.assertEquals("FAILED", readReport().get("status").asText());
  }

  @Test
  public void testUnpausesWhenFirstUserCallThrows() throws Exception {
    target.failOn = "roleSecurableObjects";
    KafkaAclImportTool.Outcome outcome = run(true, false);

    Assertions.assertEquals(1, outcome.exitCode());
    Assertions.assertEquals(ImmutableList.of(PAUSE, UNPAUSE, "triggerReconcile"), target.writes);
  }

  @Test
  public void testRefusesUnmappedDenyUnderPrefix() throws Exception {
    AclBinding hostDeny =
        new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "order_events", PatternType.LITERAL),
            new AccessControlEntry(A, "10.0.0.7", READ, DENY));
    acls.add(hostDeny);

    KafkaAclImportTool.Outcome refused = run(true, false);
    Assertions.assertEquals(2, refused.exitCode());
    Assertions.assertEquals(ImmutableList.of(), target.writes);
    Assertions.assertTrue(out.toString(StandardCharsets.UTF_8).contains(hostDeny.toString()));
    JsonNode report = readReport();
    Assertions.assertEquals("REFUSED", report.get("status").asText());
    Assertions.assertEquals(1, report.get("pluginWillDeleteDeny").size());

    KafkaAclImportTool.Outcome accepted = run(true, true);
    Assertions.assertEquals(0, accepted.exitCode(), accepted.report().toString());
    Assertions.assertTrue(target.writes.contains("createRole " + ROLE_A));
  }

  @Test
  public void testDenyUnderPrefixDoesNotBlockDryRun() throws Exception {
    acls.add(
        new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "order_", PatternType.PREFIXED),
            new AccessControlEntry(A, "*", READ, DENY)));
    Assertions.assertEquals(0, run(false, false).exitCode());
  }

  @Test
  public void testExistingRoleGrantedOrSkipped() throws Exception {
    // Same contents as planned, in a different order: granted.
    List<SecurableObject> planned =
        new ArrayList<>(
            KafkaAclImportTool.securableObjects(
                "kafka_catalog",
                KafkaAclImportPlanner.plan(
                        BASE_ACLS,
                        new KafkaAclProperties(
                            ImmutableMap.of(KafkaAclProperties.BOOTSTRAP_SERVERS, "k:9092")),
                        "analytics",
                        ImmutableSet.of("order_events", "payments", "invoices"))
                    .mapped()
                    .stream()
                    .filter(g -> g.gravitinoUser().equals("flink.order_enricher"))
                    .collect(Collectors.toList())));
    Collections.reverse(planned);
    target.roles.put(ROLE_A, planned);
    // Different contents: skipped.
    SecurableObject catalog =
        SecurableObjects.ofCatalog(
            "kafka_catalog", ImmutableList.of(Privileges.UseCatalog.allow()));
    target.roles.put(ROLE_B, ImmutableList.of(catalog));

    KafkaAclImportTool.Outcome outcome = run(true, false);
    // ROLE_B is not granted, so B's imported ACL is not regenerated; the fake cluster still holds
    // it because the fake target does not run the plugin.
    Assertions.assertEquals(0, outcome.exitCode(), outcome.report().toString());
    Assertions.assertEquals(
        ImmutableMap.of(ROLE_A, "ROLE_EXISTS_GRANTED", ROLE_B, "ROLE_EXISTS_DIFFERENT"),
        roleStatuses(readReport()));
    Assertions.assertTrue(
        target.writes.contains("grantRoleToUser " + ROLE_A + " flink.order_enricher"));
    Assertions.assertFalse(target.writes.stream().anyMatch(w -> w.startsWith("createRole")));
    Assertions.assertFalse(target.writes.stream().anyMatch(w -> w.contains(ROLE_B)));
    Assertions.assertEquals(ImmutableList.of(catalog), target.roles.get(ROLE_B));
  }

  @Test
  public void testSecondApplyCreatesNothing() throws Exception {
    run(true, false);
    List<String> firstWrites = ImmutableList.copyOf(target.writes);
    target.writes.clear();

    KafkaAclImportTool.Outcome second = run(true, false);
    Assertions.assertTrue(second.succeeded());
    Assertions.assertEquals(
        ImmutableList.of(
            PAUSE,
            "grantRoleToUser " + ROLE_B + " flink.billing-v2",
            "grantRoleToUser " + ROLE_A + " flink.order_enricher",
            UNPAUSE,
            "triggerReconcile"),
        target.writes);
    Assertions.assertTrue(firstWrites.contains("createRole " + ROLE_A));
    Assertions.assertEquals(
        ImmutableMap.of(ROLE_A, "ROLE_EXISTS_GRANTED", ROLE_B, "ROLE_EXISTS_GRANTED"),
        roleStatuses(readReport()));
  }

  @Test
  public void testRoleName() {
    Assertions.assertTrue(
        roleName("flink.order_enricher").matches("kafka_import_flink_order_enricher_[0-9a-f]{8}"));
    Assertions.assertNotEquals(roleName("a-b"), roleName("a_b"));
    Assertions.assertTrue(roleName("a-b").startsWith("kafka_import_a_b_"));
    Assertions.assertTrue(roleName("a_b").startsWith("kafka_import_a_b_"));
    Assertions.assertEquals(roleName("a-b"), roleName("a-b"));
  }

  @Test
  public void testUsageError() {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int code =
        KafkaAclImportTool.execute(
            new String[] {"--metalake", "m"},
            new PrintStream(out, true, StandardCharsets.UTF_8),
            new PrintStream(err, true, StandardCharsets.UTF_8));
    Assertions.assertEquals(2, code);
    Assertions.assertTrue(err.toString(StandardCharsets.UTF_8).contains("--gravitino-uri"));
  }

  private KafkaAclImportTool.Outcome run(boolean apply, boolean acceptDenyRemoval)
      throws Exception {
    ImportOptions options =
        new ImportOptions(
            "http://unused:8090",
            "analytics",
            "kafka_catalog",
            "admin",
            tmp.resolve("kafka.properties"),
            apply,
            acceptDenyRemoval,
            tmp.resolve("report.json"));
    return KafkaAclImportTool.run(
        options, admin, target, new PrintStream(out, true, StandardCharsets.UTF_8));
  }

  private JsonNode readReport() throws Exception {
    return new ObjectMapper().readTree(tmp.resolve("report.json").toFile());
  }

  private static Map<String, String> roleStatuses(JsonNode report) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    report.get("roles").forEach(r -> builder.put(r.get("role").asText(), r.get("status").asText()));
    return builder.build();
  }

  private static List<String> describe(List<SecurableObject> objects) {
    return objects.stream()
        .map(
            o ->
                String.format(
                    "%s %s %s",
                    o.type(),
                    o.fullName(),
                    o.privileges().stream()
                        .map(p -> p.condition() + " " + p.name())
                        .collect(Collectors.toList())))
        .collect(Collectors.toList());
  }

  private static AclBinding topic(
      String principal, String topic, AclOperation op, AclPermissionType perm) {
    return new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
        new AccessControlEntry(principal, "*", op, perm));
  }
}
