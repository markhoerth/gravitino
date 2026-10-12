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
package org.apache.gravitino.authorization.kafka.imports.integration.test;

import static org.apache.kafka.common.acl.AclOperation.ALTER;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.READ;
import static org.apache.kafka.common.acl.AclOperation.WRITE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.kafka.KafkaAclProperties;
import org.apache.gravitino.authorization.kafka.imports.GravitinoImportTarget;
import org.apache.gravitino.authorization.kafka.imports.ImportToolRunner;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * Seeds a real KRaft broker with one ACL per planner rule and runs the import tool in dry-run mode
 * against it, checking the JSON report.
 */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KafkaAclImportIT {

  private static final String IMAGE = "apache/kafka:3.8.0";
  private static final String JAAS_PATH = "/tmp/kafka_server_jaas.conf";
  private static final String P = "User:flink.order_enricher";
  private static final Set<String> CATALOG_TOPICS =
      ImmutableSet.of("order_events", "payments", "invoices");

  /** One ACL per rule a-h, with every outcome of rules d and h. */
  private static final List<AclBinding> SEEDED =
      ImmutableList.of(
          // a. NON_USER_PRINCIPAL
          topic("Group:flink.team", "order_events", READ),
          // b. OUTSIDE_PRINCIPAL_PREFIX
          topic("User:alice", "order_events", READ),
          // c. HOST_RESTRICTED
          new AclBinding(
              new ResourcePattern(ResourceType.TOPIC, "order_events", PatternType.LITERAL),
              new AccessControlEntry(P, "10.0.0.7", READ, ALLOW)),
          // d. implied own group prefix, and a foreign group
          group("flink.analytics.flink.order_enricher.", PatternType.PREFIXED),
          group("finance.", PatternType.PREFIXED),
          // e. UNSUPPORTED_RESOURCE_TYPE
          new AclBinding(
              new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL),
              new AccessControlEntry(P, "*", AclOperation.IDEMPOTENT_WRITE, ALLOW)),
          // f. NON_LITERAL_TOPIC_PATTERN
          new AclBinding(
              new ResourcePattern(ResourceType.TOPIC, "order_", PatternType.PREFIXED),
              new AccessControlEntry(P, "*", READ, ALLOW)),
          // g. TOPIC_NOT_IN_CATALOG
          topic(P, "not_in_catalog", READ),
          // h. mapped READ and WRITE, implied DESCRIBE, DESCRIBE_ONLY, UNSUPPORTED_OPERATION
          topic(P, "order_events", READ),
          topic(P, "order_events", DESCRIBE),
          topic(P, "invoices", WRITE),
          topic(P, "payments", DESCRIBE),
          topic(P, "payments", ALTER));

  @TempDir Path tmp;
  private GenericContainer<?> kafka;
  private String bootstrapServers;

  @BeforeAll
  public void startKafka() throws Exception {
    int port = freePort();
    bootstrapServers = "localhost:" + port;
    // The broker authenticates to itself (inter-broker and controller listeners) as admin.
    String serverJaas =
        "KafkaServer {\n"
            + "  org.apache.kafka.common.security.plain.PlainLoginModule required\n"
            + "  username=\"admin\" password=\"admin-secret\"\n"
            + "  user_admin=\"admin-secret\";\n"
            + "};\n";

    Map<String, String> env = new HashMap<>();
    env.put("CLUSTER_ID", "4L6g3nShT-eMCtK--X86sw");
    env.put("KAFKA_NODE_ID", "1");
    env.put("KAFKA_PROCESS_ROLES", "broker,controller");
    env.put("KAFKA_LISTENERS", "SASL_PLAINTEXT://0.0.0.0:" + port + ",CONTROLLER://localhost:9093");
    env.put("KAFKA_ADVERTISED_LISTENERS", "SASL_PLAINTEXT://localhost:" + port);
    env.put(
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
        "CONTROLLER:SASL_PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT");
    env.put("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
    env.put("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093");
    env.put("KAFKA_INTER_BROKER_LISTENER_NAME", "SASL_PLAINTEXT");
    env.put("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN");
    env.put("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN");
    env.put("KAFKA_SASL_MECHANISM_CONTROLLER_PROTOCOL", "PLAIN");
    env.put("KAFKA_OPTS", "-Djava.security.auth.login.config=" + JAAS_PATH);
    env.put(
        "KAFKA_AUTHORIZER_CLASS_NAME", "org.apache.kafka.metadata.authorizer.StandardAuthorizer");
    env.put("KAFKA_SUPER_USERS", "User:admin");
    env.put("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false");
    env.put("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");

    kafka =
        new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withEnv(env)
            .withCopyToContainer(Transferable.of(serverJaas), JAAS_PATH)
            .withExposedPorts(port)
            .withCreateContainerCmdModifier(
                cmd ->
                    cmd.getHostConfig()
                        .withPortBindings(
                            new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(port))))
            .waitingFor(
                Wait.forLogMessage(".*Kafka Server started.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    kafka.start();

    try (Admin admin = Admin.create(adminProps())) {
      admin.createAcls(SEEDED).all().get();
    }
  }

  @AfterAll
  public void stopKafka() {
    if (kafka != null) {
      kafka.stop();
    }
  }

  @Test
  public void testDryRunReport() throws Exception {
    Path kafkaConfig = tmp.resolve("kafka.properties");
    try (OutputStream out = Files.newOutputStream(kafkaConfig)) {
      adminProps().store(out, null);
    }
    Path reportPath = tmp.resolve("report.json");
    ReadOnlyTarget target = new ReadOnlyTarget();
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();

    int code =
        ImportToolRunner.run(
            new String[] {
              "--gravitino-uri", "http://unused:8090",
              "--metalake", "analytics",
              "--catalog", "kafka_catalog",
              "--user", "admin",
              "--kafka-config", kafkaConfig.toString(),
              "--report", reportPath.toString()
            },
            target,
            new PrintStream(stdout, true, StandardCharsets.UTF_8));

    Assertions.assertEquals(0, code, stdout.toString(StandardCharsets.UTF_8));
    JsonNode report = new ObjectMapper().readTree(reportPath.toFile());
    Assertions.assertEquals("dry-run", report.get("mode").asText());
    Assertions.assertEquals(SEEDED.size(), report.at("/summary/acls").asInt());
    Assertions.assertEquals(2, report.at("/summary/mapped").asInt());
    Assertions.assertEquals(2, report.at("/summary/implied").asInt());
    Assertions.assertEquals(9, report.at("/summary/unmapped").asInt());
    Assertions.assertEquals(
        ImmutableMap.builder()
            .put("NON_USER_PRINCIPAL", 1)
            .put("OUTSIDE_PRINCIPAL_PREFIX", 1)
            .put("HOST_RESTRICTED", 1)
            .put("GROUP_NOT_PRINCIPAL_PREFIX", 1)
            .put("UNSUPPORTED_RESOURCE_TYPE", 1)
            .put("NON_LITERAL_TOPIC_PATTERN", 1)
            .put("TOPIC_NOT_IN_CATALOG", 1)
            .put("UNSUPPORTED_OPERATION", 1)
            .put("DESCRIBE_ONLY", 1)
            .build(),
        counts(report.at("/summary/unmappedByReason")));
    // Everything under the prefix that is unmapped is what the plugin would delete.
    Assertions.assertEquals(7, report.at("/summary/pluginWillDelete").asInt());
    Assertions.assertFalse(target.wrote, "dry run must not write to Gravitino");
  }

  private static Map<Object, Object> counts(JsonNode node) {
    ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder();
    for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> e = it.next();
      builder.put(e.getKey(), e.getValue().asInt());
    }
    return builder.build();
  }

  private Properties adminProps() {
    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    props.put(
        SaslConfigs.SASL_JAAS_CONFIG,
        "org.apache.kafka.common.security.plain.PlainLoginModule required"
            + " username=\"admin\" password=\"admin-secret\";");
    return props;
  }

  private static AclBinding topic(String principal, String topic, AclOperation op) {
    return new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
        new AccessControlEntry(principal, "*", op, ALLOW));
  }

  private static AclBinding group(String name, PatternType type) {
    return new AclBinding(
        new ResourcePattern(ResourceType.GROUP, name, type),
        new AccessControlEntry(P, "*", READ, ALLOW));
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /** Returns the seeded catalog's topics and properties; fails the test on any write. */
  private class ReadOnlyTarget implements GravitinoImportTarget {
    private boolean wrote;

    @Override
    public Map<String, String> catalogProperties() {
      return ImmutableMap.of(KafkaAclProperties.BOOTSTRAP_SERVERS, bootstrapServers);
    }

    @Override
    public Set<String> listTopics() {
      return CATALOG_TOPICS;
    }

    @Override
    public boolean userExists(String user) {
      return false;
    }

    @Override
    public void addUser(String user) {
      wrote = true;
    }

    @Override
    public Optional<List<SecurableObject>> roleSecurableObjects(String role) {
      return Optional.empty();
    }

    @Override
    public void createRole(String role, List<SecurableObject> securableObjects) {
      wrote = true;
    }

    @Override
    public void grantRoleToUser(String role, String user) {
      wrote = true;
    }

    @Override
    public void setCatalogProperty(String key, String value) {
      wrote = true;
    }

    @Override
    public String triggerReconcile() {
      wrote = true;
      return "none";
    }

    @Override
    public void close() {}
  }
}
