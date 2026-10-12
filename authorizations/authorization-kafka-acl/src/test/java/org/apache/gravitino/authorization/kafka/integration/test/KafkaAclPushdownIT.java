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
package org.apache.gravitino.authorization.kafka.integration.test;

import static org.apache.gravitino.authorization.Privilege.Name.CONSUME_TOPIC;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.authorization.kafka.KafkaAclAuthorizationPlugin;
import org.apache.gravitino.authorization.kafka.KafkaAclProperties;
import org.apache.gravitino.authorization.kafka.KafkaGrant;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * Drives {@link KafkaAclAuthorizationPlugin} against a real KRaft broker with StandardAuthorizer
 * and checks that the Broker enforces the pushed-down ACLs for a client that never calls Gravitino.
 */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KafkaAclPushdownIT {

  private static final String IMAGE = "apache/kafka:3.8.0";
  private static final String METALAKE = "analytics";
  private static final String PRINCIPAL = "User:flink.order_enricher";
  private static final String ORDER_EVENTS = "order_events";
  private static final String OTHER_TOPIC = "other_topic";
  private static final String GOOD_GROUP = "flink.analytics.flink.order_enricher.prod";
  private static final String JAAS_PATH = "/tmp/kafka_server_jaas.conf";
  private static final Duration PROPAGATION_TIMEOUT = Duration.ofSeconds(30);

  private final List<KafkaGrant> grants = new CopyOnWriteArrayList<>();
  private GenericContainer<?> kafka;
  private String bootstrapServers;
  private KafkaAclAuthorizationPlugin plugin;

  @BeforeAll
  public void startKafka() throws Exception {
    int port = freePort();
    bootstrapServers = "localhost:" + port;
    // The broker authenticates to itself (inter-broker and controller listeners) as admin.
    String serverJaas =
        "KafkaServer {\n"
            + "  org.apache.kafka.common.security.plain.PlainLoginModule required\n"
            + "  username=\"admin\" password=\"admin-secret\"\n"
            + "  user_admin=\"admin-secret\"\n"
            + "  user_flink.order_enricher=\"enricher-secret\"\n"
            + "  user_flink.other=\"other-secret\";\n"
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
    env.put("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
    env.put("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
    env.put("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");

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

    try (Admin admin = Admin.create(clientProps("admin", "admin-secret"))) {
      admin
          .createTopics(
              ImmutableList.of(
                  new NewTopic(ORDER_EVENTS, 1, (short) 1),
                  new NewTopic(OTHER_TOPIC, 1, (short) 1)))
          .all()
          .get();
    }
    produce(ORDER_EVENTS, "seed");
    produce(OTHER_TOPIC, "seed");

    Map<String, String> config =
        ImmutableMap.<String, String>builder()
            .put(KafkaAclProperties.BOOTSTRAP_SERVERS, bootstrapServers)
            .put(
                KafkaAclProperties.ADMIN_PREFIX + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                "SASL_PLAINTEXT")
            .put(KafkaAclProperties.ADMIN_PREFIX + SaslConfigs.SASL_MECHANISM, "PLAIN")
            .put(
                KafkaAclProperties.ADMIN_PREFIX + SaslConfigs.SASL_JAAS_CONFIG,
                jaas("admin", "admin-secret"))
            .build();
    plugin =
        new KafkaAclAuthorizationPlugin(METALAKE, config, pending -> KafkaGrant.effective(grants));
  }

  @AfterAll
  public void stopKafka() throws IOException {
    if (plugin != null) {
      plugin.close();
    }
    if (kafka != null) {
      kafka.stop();
    }
  }

  @BeforeEach
  public void resetGrants() {
    grants.clear();
    plugin.onRevokedRolesFromUser(ImmutableList.of(), user());
  }

  @Test
  public void testNoGrantDenied() {
    try (KafkaConsumer<String, String> consumer = enricherConsumer(null)) {
      consumer.assign(ImmutableList.of(new TopicPartition(ORDER_EVENTS, 0)));
      consumer.seekToBeginning(consumer.assignment());
      assertPollThrows(consumer, TopicAuthorizationException.class);
    }
  }

  @Test
  public void testGrantedConsumerReadsAndCommits() {
    grantConsume(ORDER_EVENTS, KafkaGrant.Effect.ALLOW);
    String value = "order-" + UUID.randomUUID();
    produce(ORDER_EVENTS, value);

    try (KafkaConsumer<String, String> consumer = enricherConsumer(GOOD_GROUP)) {
      consumer.subscribe(ImmutableList.of(ORDER_EVENTS));
      Assertions.assertTrue(pollUntilValue(consumer, value), "record not consumed");
      consumer.commitSync();
      Assertions.assertNotNull(
          consumer
              .committed(Collections.singleton(new TopicPartition(ORDER_EVENTS, 0)))
              .get(new TopicPartition(ORDER_EVENTS, 0)));
    }
  }

  @Test
  public void testGroupOutsidePrefixDenied() {
    grantConsume(ORDER_EVENTS, KafkaGrant.Effect.ALLOW);

    try (KafkaConsumer<String, String> consumer = enricherConsumer("finance.monthly_close")) {
      consumer.subscribe(ImmutableList.of(ORDER_EVENTS));
      assertPollThrows(consumer, GroupAuthorizationException.class);
    }
  }

  @Test
  public void testUngrantedTopicDenied() {
    grantConsume(ORDER_EVENTS, KafkaGrant.Effect.ALLOW);

    try (KafkaConsumer<String, String> consumer = enricherConsumer(null)) {
      consumer.assign(ImmutableList.of(new TopicPartition(OTHER_TOPIC, 0)));
      consumer.seekToBeginning(consumer.assignment());
      assertPollThrows(consumer, TopicAuthorizationException.class);
    }
  }

  @Test
  public void testRevokeDeniesNextPoll() {
    grantConsume(ORDER_EVENTS, KafkaGrant.Effect.ALLOW);
    String value = "order-" + UUID.randomUUID();
    produce(ORDER_EVENTS, value);

    try (KafkaConsumer<String, String> consumer = enricherConsumer(null)) {
      consumer.assign(ImmutableList.of(new TopicPartition(ORDER_EVENTS, 0)));
      consumer.seekToBeginning(consumer.assignment());
      Assertions.assertTrue(pollUntilValue(consumer, value), "record not consumed before revoke");

      grants.clear();
      Assertions.assertEquals(
          Boolean.TRUE, plugin.onRevokedRolesFromUser(ImmutableList.of(), user()));
      produce(ORDER_EVENTS, "after-revoke");
      assertPollThrows(consumer, TopicAuthorizationException.class);
    }
  }

  @Test
  public void testDenyInSecondRoleWins() {
    grantConsume(ORDER_EVENTS, KafkaGrant.Effect.ALLOW);
    String value = "order-" + UUID.randomUUID();
    produce(ORDER_EVENTS, value);

    try (KafkaConsumer<String, String> consumer = enricherConsumer(null)) {
      consumer.assign(ImmutableList.of(new TopicPartition(ORDER_EVENTS, 0)));
      consumer.seekToBeginning(consumer.assignment());
      Assertions.assertTrue(pollUntilValue(consumer, value), "record not consumed before DENY");

      // A second role carries a DENY on the same topic; the ALLOW from the first role stays.
      grantConsume(ORDER_EVENTS, KafkaGrant.Effect.DENY);
      produce(ORDER_EVENTS, "after-deny");
      assertPollThrows(consumer, TopicAuthorizationException.class);
    }
  }

  private void grantConsume(String topic, KafkaGrant.Effect effect) {
    grants.add(KafkaGrant.topic(PRINCIPAL, topic, CONSUME_TOPIC, effect));
    Assertions.assertEquals(
        Boolean.TRUE,
        plugin.onGrantedRolesToUser(ImmutableList.of(Mockito.mock(Role.class)), user()));
  }

  private static User user() {
    User user = Mockito.mock(User.class);
    Mockito.when(user.name()).thenReturn("flink.order_enricher");
    return user;
  }

  private static boolean pollUntilValue(KafkaConsumer<String, String> consumer, String value) {
    long deadline = System.nanoTime() + PROPAGATION_TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
      for (var record : records) {
        if (value.equals(record.value())) {
          return true;
        }
      }
    }
    return false;
  }

  /** ACL changes reach the broker through the metadata log, so allow a short propagation delay. */
  private static void assertPollThrows(
      KafkaConsumer<String, String> consumer, Class<? extends RuntimeException> expected) {
    long deadline = System.nanoTime() + PROPAGATION_TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      try {
        consumer.poll(Duration.ofMillis(500));
      } catch (RuntimeException e) {
        Assertions.assertInstanceOf(expected, e, "unexpected exception: " + e);
        return;
      }
    }
    Assertions.fail(
        "poll did not throw " + expected.getSimpleName() + " within " + PROPAGATION_TIMEOUT);
  }

  private KafkaConsumer<String, String> enricherConsumer(String groupId) {
    Properties props = clientProps("flink.order_enricher", "enricher-secret");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    if (groupId != null) {
      props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }
    return new KafkaConsumer<>(props);
  }

  private void produce(String topic, String value) {
    Properties props = clientProps("admin", "admin-secret");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      producer.send(new ProducerRecord<>(topic, value)).get();
    } catch (Exception e) {
      throw new RuntimeException("Failed to produce to " + topic, e);
    }
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
}
