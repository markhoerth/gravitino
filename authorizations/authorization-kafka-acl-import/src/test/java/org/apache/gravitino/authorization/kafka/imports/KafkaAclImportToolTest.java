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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.authorization.SecurableObject;
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
  private static final List<AclBinding> ACLS =
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
              when(result.values()).thenReturn(KafkaFuture.completedFuture(ACLS));
              return result;
            });
    out = new ByteArrayOutputStream();
  }

  @Test
  public void testDryRunMakesNoWrites() throws Exception {
    KafkaAclImportTool.Outcome outcome = run(false);

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
    KafkaAclImportTool.Outcome outcome = run(true);

    Assertions.assertTrue(outcome.succeeded(), outcome.report().toString());
    Assertions.assertEquals(
        ImmutableList.of(
            "createRole kafka_import_flink_billing_v2",
            "grantRoleToUser kafka_import_flink_billing_v2 flink.billing-v2",
            "addUser flink.order_enricher",
            "createRole kafka_import_flink_order_enricher",
            "grantRoleToUser kafka_import_flink_order_enricher flink.order_enricher"),
        target.writes);

    List<SecurableObject> objects = target.roles.get("kafka_import_flink_order_enricher");
    Assertions.assertEquals(
        ImmutableList.of(
            "CATALOG kafka_catalog [ALLOW USE_CATALOG]",
            "SCHEMA kafka_catalog.default [ALLOW USE_SCHEMA]",
            "TOPIC kafka_catalog.default.order_events [ALLOW CONSUME_TOPIC]",
            "TOPIC kafka_catalog.default.payments [DENY CONSUME_TOPIC]"),
        describe(objects));
    Assertions.assertEquals(
        ImmutableList.of(
            "CATALOG kafka_catalog [ALLOW USE_CATALOG]",
            "SCHEMA kafka_catalog.default [ALLOW USE_SCHEMA]",
            "TOPIC kafka_catalog.default.invoices [ALLOW PRODUCE_TOPIC]"),
        describe(target.roles.get("kafka_import_flink_billing_v2")));

    JsonNode report = readReport();
    Assertions.assertEquals("OK", report.get("status").asText());
    Assertions.assertEquals(0, report.get("missingAfterApply").size());
    for (JsonNode role : report.get("roles")) {
      Assertions.assertEquals("CREATED", role.get("status").asText());
    }
  }

  @Test
  public void testExistingRoleSkippedAndReported() throws Exception {
    target.roles.put("kafka_import_flink_order_enricher", ImmutableList.of());
    run(true);

    Assertions.assertEquals(
        ImmutableList.of(
                "createRole kafka_import_flink_billing_v2",
                "addUser flink.billing-v2",
                "grantRoleToUser kafka_import_flink_billing_v2 flink.billing-v2")
            .stream()
            .sorted()
            .collect(Collectors.toList()),
        target.writes.stream().sorted().collect(Collectors.toList()));
    Assertions.assertEquals(
        ImmutableList.of(), target.roles.get("kafka_import_flink_order_enricher"));
    Assertions.assertNull(target.grants.get("flink.order_enricher"));
    Assertions.assertEquals(
        ImmutableMap.of(
            "kafka_import_flink_billing_v2", "CREATED",
            "kafka_import_flink_order_enricher", "ROLE_EXISTS"),
        roleStatuses(readReport()));
  }

  @Test
  public void testSecondApplyIsNoOp() throws Exception {
    run(true);
    int writes = target.writes.size();
    Assertions.assertTrue(writes > 0);

    KafkaAclImportTool.Outcome second = run(true);
    Assertions.assertTrue(second.succeeded());
    Assertions.assertEquals(writes, target.writes.size(), target.writes.toString());
    for (String status : roleStatuses(readReport()).values()) {
      Assertions.assertEquals("ROLE_EXISTS", status);
    }
  }

  @Test
  public void testRoleName() {
    Assertions.assertEquals(
        "kafka_import_flink_order_enricher", KafkaAclImportTool.roleName("flink.order_enricher"));
    Assertions.assertEquals("kafka_import_a_b_c_9", KafkaAclImportTool.roleName("a-b@c.9"));
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

  private KafkaAclImportTool.Outcome run(boolean apply) throws Exception {
    ImportOptions options =
        new ImportOptions(
            "http://unused:8090",
            "analytics",
            "kafka_catalog",
            "admin",
            tmp.resolve("kafka.properties"),
            apply,
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
