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

import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclOperation.READ;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.authorization.kafka.KafkaAclAuthorization;
import org.apache.gravitino.authorization.kafka.KafkaAclAuthorizationPlugin;
import org.apache.gravitino.authorization.kafka.KafkaAclProperties;
import org.apache.gravitino.authorization.kafka.KafkaGrant;
import org.apache.gravitino.authorization.kafka.imports.GravitinoImportTarget;
import org.apache.gravitino.authorization.kafka.imports.ImportToolRunner;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs {@code --apply} against a real broker with the real plugin, wired to a fake Gravitino that
 * fires the same hooks core fires, and samples consumer access for every imported user after every
 * Gravitino call. With the reconcile pause, no imported user may lose access at any point.
 */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KafkaAclImportApplyIT {

  private static final String IMAGE = "apache/kafka:3.8.0";
  private static final String JAAS_PATH = "/tmp/kafka_server_jaas.conf";
  private static final String METALAKE = "analytics";
  private static final String CATALOG = "kafka_catalog";
  private static final List<String> USERS =
      ImmutableList.of("flink.alpha", "flink.bravo", "flink.charlie");
  private static final Duration SAMPLE_TIMEOUT = Duration.ofSeconds(10);

  @TempDir Path tmp;
  private GenericContainer<?> kafka;
  private String bootstrapServers;
  private Admin admin;
  private final Map<String, KafkaConsumer<String, String>> consumers = new LinkedHashMap<>();

  @BeforeAll
  public void startKafka() throws Exception {
    int port = freePort();
    bootstrapServers = "localhost:" + port;
    StringBuilder jaas =
        new StringBuilder("KafkaServer {\n")
            .append("  org.apache.kafka.common.security.plain.PlainLoginModule required\n")
            .append("  username=\"admin\" password=\"admin-secret\"\n")
            .append("  user_admin=\"admin-secret\"");
    for (String user : USERS) {
      jaas.append("\n  user_").append(user).append("=\"").append(password(user)).append("\"");
    }
    jaas.append(";\n};\n");

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
            .withCopyToContainer(Transferable.of(jaas.toString()), JAAS_PATH)
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

    admin = Admin.create(clientProps("admin", "admin-secret"));
    List<NewTopic> topics = new ArrayList<>();
    USERS.forEach(u -> topics.add(new NewTopic(topic(u), 1, (short) 1)));
    admin.createTopics(topics).all().get();
    Properties producerProps = clientProps("admin", "admin-secret");
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
      for (String user : USERS) {
        producer.send(new ProducerRecord<>(topic(user), "seed-" + user)).get();
      }
    }
  }

  @AfterAll
  public void stopKafka() {
    if (admin != null) {
      admin.close();
    }
    if (kafka != null) {
      kafka.stop();
    }
  }

  @BeforeEach
  public void seedAcls() throws Exception {
    admin.deleteAcls(ImmutableList.of(AclBindingFilter.ANY)).all().get();
    admin.createAcls(seeded()).all().get();
    for (String user : USERS) {
      Assertions.assertEquals(Optional.empty(), sample(user), "seeded access broken for " + user);
    }
  }

  @AfterEach
  public void closeConsumers() {
    consumers.values().forEach(KafkaConsumer::close);
    consumers.clear();
  }

  @Test
  public void testApplyKeepsAccessAtEveryStep() throws Exception {
    PluginBackedTarget target = new PluginBackedTarget(true);
    List<String> errors = new ArrayList<>();
    target.afterCall = call -> errors.addAll(sampleAll(call));

    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    Path report = tmp.resolve("report.json");
    int code =
        ImportToolRunner.run(
            args(report), target, new PrintStream(stdout, true, StandardCharsets.UTF_8));

    String output = stdout.toString(StandardCharsets.UTF_8);
    Assertions.assertEquals(0, code, output);
    Assertions.assertTrue(target.calls.size() >= 20, target.calls.toString());
    Assertions.assertEquals(ImmutableList.of(), errors, "authorization errors during apply");

    JsonNode json = new ObjectMapper().readTree(report.toFile());
    Assertions.assertEquals("OK", json.get("status").asText());
    Assertions.assertEquals(0, json.get("missingAfterApply").size());
    Assertions.assertEquals("onOwnerSet (fake)", json.get("reconcileTrigger").asText());
    json.get("roles").forEach(r -> Assertions.assertEquals("CREATED", r.get("status").asText()));

    // The last call is the trigger; the plugin is unpaused and has reconciled the final state.
    Assertions.assertEquals(KafkaAclAuthorizationPlugin.Health.HEALTHY, target.plugin.health());
    Assertions.assertTrue(target.pausedHooksSkipped > 0, "hooks should have fired while paused");
    Assertions.assertEquals(ImmutableSet.copyOf(seeded()), ImmutableSet.copyOf(currentAcls()));
    target.close();
  }

  @Test
  public void testWithoutPauseTheOutageIsDetected() throws Exception {
    // Control: a plugin that ignores the pause flag shows the sampling catches the outage.
    PluginBackedTarget target = new PluginBackedTarget(false);
    List<String> errors = new ArrayList<>();
    target.afterCall = call -> errors.addAll(sampleAll(call));

    ImportToolRunner.run(
        args(tmp.resolve("control.json")),
        target,
        new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));

    Assertions.assertFalse(errors.isEmpty(), "control run should observe lost access");
    target.close();
  }

  private String[] args(Path report) throws IOException {
    Path kafkaConfig = tmp.resolve("kafka.properties");
    try (OutputStream out = Files.newOutputStream(kafkaConfig)) {
      clientProps("admin", "admin-secret").store(out, null);
    }
    return new String[] {
      "--gravitino-uri",
      "http://unused:8090",
      "--metalake",
      METALAKE,
      "--catalog",
      CATALOG,
      "--user",
      "admin",
      "--kafka-config",
      kafkaConfig.toString(),
      "--report",
      report.toString(),
      "--apply"
    };
  }

  /** Checks every user's seeded ACLs are present and every user can still read its topic. */
  private List<String> sampleAll(String call) {
    List<String> errors = new ArrayList<>();
    Set<AclBinding> now = new HashSet<>(currentAcls());
    for (AclBinding acl : seeded()) {
      if (!now.contains(acl)) {
        errors.add("after " + call + ": ACL missing " + acl);
      }
    }
    for (String user : USERS) {
      sample(user).ifPresent(e -> errors.add("after " + call + ": " + user + " " + e));
    }
    return errors;
  }

  /** Reads one record of the user's topic; returns the authorization error if access is lost. */
  private Optional<String> sample(String user) {
    KafkaConsumer<String, String> consumer = consumers.computeIfAbsent(user, this::newConsumer);
    TopicPartition partition = new TopicPartition(topic(user), 0);
    try {
      consumer.seekToBeginning(ImmutableList.of(partition));
      long deadline = System.nanoTime() + SAMPLE_TIMEOUT.toNanos();
      while (System.nanoTime() < deadline) {
        if (!consumer.poll(Duration.ofMillis(200)).isEmpty()) {
          return Optional.empty();
        }
      }
      return Optional.of("no record within " + SAMPLE_TIMEOUT);
    } catch (TopicAuthorizationException e) {
      consumers.remove(user).close();
      return Optional.of(e.toString());
    }
  }

  private KafkaConsumer<String, String> newConsumer(String user) {
    Properties props = clientProps(user, password(user));
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.assign(ImmutableList.of(new TopicPartition(topic(user), 0)));
    return consumer;
  }

  private Collection<AclBinding> currentAcls() {
    try {
      return admin.describeAcls(AclBindingFilter.ANY).values().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static List<AclBinding> seeded() {
    List<AclBinding> acls = new ArrayList<>();
    for (String user : USERS) {
      String principal = "User:" + user;
      acls.add(topicAcl(principal, topic(user), READ));
      acls.add(topicAcl(principal, topic(user), DESCRIBE));
      acls.add(
          new AclBinding(
              new ResourcePattern(
                  ResourceType.GROUP, "flink." + METALAKE + "." + user + ".", PatternType.PREFIXED),
              new AccessControlEntry(principal, "*", READ, ALLOW)));
    }
    return acls;
  }

  private static AclBinding topicAcl(String principal, String topic, AclOperation op) {
    return new AclBinding(
        new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
        new AccessControlEntry(principal, "*", op, ALLOW));
  }

  private static String topic(String user) {
    return "t_" + user.substring(user.indexOf('.') + 1);
  }

  private static String password(String user) {
    return user + "-secret";
  }

  private Properties clientProps(String user, String password) {
    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    props.put(SaslConfigs.SASL_JAAS_CONFIG, jaas(user, password));
    return props;
  }

  private static String jaas(String user, String password) {
    return String.format(
        "org.apache.kafka.common.security.plain.PlainLoginModule required"
            + " username=\"%s\" password=\"%s\";",
        user, password);
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /**
   * A fake Gravitino that fires the plugin hooks core fires for the same calls, and rebuilds the
   * plugin on a catalog property change as core's alterCatalog does.
   */
  private class PluginBackedTarget implements GravitinoImportTarget {
    private final boolean honorPause;
    private final Map<String, String> catalogProperties = new HashMap<>();
    private final Set<String> users = new HashSet<>();
    private final Map<String, List<SecurableObject>> roles = new HashMap<>();
    private final Map<String, Set<String>> grants = new HashMap<>();
    private final List<String> calls = new ArrayList<>();
    private Consumer<String> afterCall = call -> {};
    private KafkaAclAuthorizationPlugin plugin;
    private int pausedHooksSkipped;

    PluginBackedTarget(boolean honorPause) {
      this.honorPause = honorPause;
      catalogProperties.put("authorization-provider", KafkaAclAuthorization.SHORT_NAME);
      catalogProperties.put(KafkaAclProperties.BOOTSTRAP_SERVERS, bootstrapServers);
      Properties adminProps = clientProps("admin", "admin-secret");
      for (String key : adminProps.stringPropertyNames()) {
        if (!key.equals(KafkaAclProperties.BOOTSTRAP_SERVERS)) {
          catalogProperties.put(KafkaAclProperties.ADMIN_PREFIX + key, adminProps.getProperty(key));
        }
      }
      plugin = newPlugin();
    }

    private KafkaAclAuthorizationPlugin newPlugin() {
      Map<String, String> conf = new HashMap<>(catalogProperties);
      if (!honorPause) {
        conf.remove(KafkaAclProperties.RECONCILE_PAUSED);
      }
      return new KafkaAclAuthorizationPlugin(METALAKE, conf, pending -> currentGrants());
    }

    private List<KafkaGrant> currentGrants() {
      List<KafkaGrant> result = new ArrayList<>();
      grants.forEach(
          (user, granted) -> {
            for (String role : granted) {
              for (SecurableObject object : roles.get(role)) {
                if (object.type() != MetadataObject.Type.TOPIC) {
                  continue;
                }
                for (Privilege p : object.privileges()) {
                  result.add(
                      KafkaGrant.topic(
                          "User:" + user,
                          object.name(),
                          p.name(),
                          p.condition() == Privilege.Condition.DENY
                              ? KafkaGrant.Effect.DENY
                              : KafkaGrant.Effect.ALLOW));
                }
              }
            }
          });
      return KafkaGrant.effective(result).stream().collect(ImmutableList.toImmutableList());
    }

    private void fire(Runnable hook) {
      if (plugin.health() == KafkaAclAuthorizationPlugin.Health.PAUSED) {
        pausedHooksSkipped++;
      }
      hook.run();
    }

    private <T> T done(String call, T result) {
      calls.add(call);
      afterCall.accept(call);
      return result;
    }

    @Override
    public Map<String, String> catalogProperties() {
      return done("catalogProperties", ImmutableMap.copyOf(catalogProperties));
    }

    @Override
    public Set<String> listTopics() {
      Set<String> topics = new HashSet<>();
      USERS.forEach(u -> topics.add(topic(u)));
      return done("listTopics", topics);
    }

    @Override
    public boolean userExists(String user) {
      return done("userExists " + user, users.contains(user));
    }

    @Override
    public void addUser(String user) {
      users.add(user);
      done("addUser " + user, null);
    }

    @Override
    public Optional<List<SecurableObject>> roleSecurableObjects(String role) {
      return done("roleSecurableObjects " + role, Optional.ofNullable(roles.get(role)));
    }

    @Override
    public void createRole(String role, List<SecurableObject> securableObjects) {
      roles.put(role, securableObjects);
      fire(() -> plugin.onRoleCreated(Mockito.mock(Role.class)));
      done("createRole " + role, null);
    }

    @Override
    public void grantRoleToUser(String role, String user) {
      grants.computeIfAbsent(user, u -> new HashSet<>()).add(role);
      fire(
          () ->
              plugin.onGrantedRolesToUser(
                  ImmutableList.of(Mockito.mock(Role.class)), Mockito.mock(User.class)));
      done("grantRoleToUser " + role + " " + user, null);
    }

    @Override
    public void setCatalogProperty(String key, String value) {
      catalogProperties.put(key, value);
      try {
        plugin.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      plugin = newPlugin();
      done("setCatalogProperty " + key + "=" + value, null);
    }

    @Override
    public String triggerReconcile() {
      fire(() -> plugin.onOwnerSet(Mockito.mock(MetadataObject.class), null, null));
      return done("triggerReconcile", "onOwnerSet (fake)");
    }

    @Override
    public void close() throws IOException {
      plugin.close();
    }
  }
}
