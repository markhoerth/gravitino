/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.elasticsearch.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.elasticsearch.ElasticsearchCatalogOperations;
import org.apache.gravitino.catalog.elasticsearch.ElasticsearchCatalogPropertiesMetadata;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Covers basic authentication and certificate verification against a secured cluster.
 *
 * <p>{@code CatalogElasticsearchIT} runs with security switched off, which exercises the mapping
 * path but never sends a credential or negotiates TLS. The target deployment requires both, so the
 * credentialed path, both ways of getting it wrong, and both settings of {@code
 * elasticsearch.tls.skip-verify} are covered here.
 *
 * <p>The cluster serves TLS with a certificate no client trusts. Setting {@code
 * xpack.security.enabled} explicitly suppresses the image's security auto configuration, so it
 * would otherwise serve plain HTTP; the certificate is generated up front with {@code
 * elasticsearch-certutil --self-signed} and copied into the cluster's config directory. A self
 * signed certificate is also the more useful fixture, since it is what an internal cluster
 * typically presents.
 */
@EnabledIf("dockerAvailable")
public class CatalogElasticsearchAuthIT {

  /** The version under test, overridable so a run can check an older major. */
  private static final String VERSION = System.getProperty("elasticsearch.test.version", "9.2.4");

  private static final String IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:" + VERSION;

  private static final int ES_PORT = 9200;

  private static final String INDEX = "secured_fixture";

  private static final String USER = "elastic";

  private static final String PASSWORD = "changeme";

  private static final String CONFIG_CERTS = "/usr/share/elasticsearch/config/certs/";

  private static final Namespace NAMESPACE = Namespace.of("metalake", "catalog", "default");

  private static final String MAPPING =
      "{\"mappings\":{\"properties\":{\"id\":{\"type\":\"keyword\"}}}}";

  /** Names the generated certificate, and with it the paths certutil writes it to. */
  private static final String CERT_NAME = "elasticsearch-test";

  /**
   * Generates a self signed certificate, then idles long enough to be copied out of. The image's
   * entrypoint execs any command that is not {@code eswrapper}, so no entrypoint override is
   * needed.
   *
   * <p>The subject alternative names matter. Testcontainers dials {@code localhost} on a mapped
   * port, so a certificate without them would fail the handshake on the subject name before the
   * trust anchor was ever consulted, and {@link #testTlsVerifyRejectsSelfSigned} would pass without
   * exercising the condition it exists to pin.
   */
  private static final String GENERATE_CERT =
      "set -e; "
          + "bin/elasticsearch-certutil cert --self-signed --silent --pem --name "
          + CERT_NAME
          + " --dns localhost --ip 127.0.0.1 --out /tmp/http.zip; "
          + "unzip -o -q /tmp/http.zip -d /tmp/certs; "
          + "echo CERT_READY; "
          + "sleep 300";

  /** Where certutil leaves the PEM pair, named after {@link #CERT_NAME}. */
  private static final String GENERATED_DIR = "/tmp/certs/" + CERT_NAME + "/" + CERT_NAME;

  private static GenericContainer<?> container;

  private static String uri;

  /**
   * Guards the whole class, so a machine without a reachable Docker daemon skips rather than fails.
   *
   * @return true if testcontainers can reach a Docker daemon
   */
  static boolean dockerAvailable() {
    try {
      return DockerClientFactory.instance().isDockerAvailable();
    } catch (Throwable t) {
      return false;
    }
  }

  /**
   * Starts a single node cluster that demands a credential and serves TLS with an untrusted
   * certificate, then creates the fixture index as the superuser.
   *
   * @throws Exception if the certificate cannot be generated, the container does not start, or the
   *     index cannot be created
   */
  @BeforeAll
  public static void startCluster() throws Exception {
    Path certs = Files.createTempDirectory("gravitino-es-tls");
    certs.toFile().deleteOnExit();
    Path certificate = certs.resolve("http.crt");
    Path key = certs.resolve("http.key");
    generateCertificate(certificate, key);

    container =
        new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withExposedPorts(ES_PORT)
            .withEnv("discovery.type", "single-node")
            .withEnv("xpack.security.enabled", "true")
            .withEnv("xpack.security.http.ssl.enabled", "true")
            .withEnv("xpack.security.http.ssl.key", "certs/http.key")
            .withEnv("xpack.security.http.ssl.certificate", "certs/http.crt")
            .withEnv("ELASTIC_PASSWORD", PASSWORD)
            .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
            .withCopyFileToContainer(
                MountableFile.forHostPath(certificate, 0644), CONFIG_CERTS + "http.crt")
            .withCopyFileToContainer(
                MountableFile.forHostPath(key, 0644), CONFIG_CERTS + "http.key")
            .waitingFor(
                Wait.forHttp("/")
                    .usingTls()
                    .allowInsecure()
                    .withBasicCredentials(USER, PASSWORD)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(5)));
    container.start();

    uri = "https://" + container.getHost() + ":" + container.getMappedPort(ES_PORT);
    createFixtureIndex();
  }

  /** Stops the cluster. */
  @AfterAll
  public static void stopCluster() {
    if (container != null) {
      container.stop();
      container = null;
    }
  }

  /**
   * A catalog carrying the right credential reads the cluster over TLS.
   *
   * @throws Exception if the catalog cannot be closed
   */
  @Test
  public void testAuthenticatedListTables() throws Exception {
    assertTrue(uri.startsWith("https://"), "the cluster is not serving TLS: " + uri);

    try (ElasticsearchCatalogOperations operations = catalog(USER, PASSWORD, true)) {
      Set<String> tables =
          Arrays.stream(operations.listTables(NAMESPACE))
              .map(NameIdentifier::name)
              .collect(Collectors.toCollection(TreeSet::new));

      assertTrue(tables.contains(INDEX), "listTables did not report " + INDEX + ", got " + tables);
    }
  }

  /**
   * With verification left on, the cluster's self signed certificate is rejected.
   *
   * <p>This pins a limitation rather than a feature. The catalog has no property naming a CA
   * certificate or a truststore, so the only way to reach a cluster presenting a certificate the
   * JVM does not trust is to turn verification off entirely with {@code
   * elasticsearch.tls.skip-verify}. Should such a property ever be added, this test is where the
   * gap is recorded.
   *
   * @throws Exception if the catalog cannot be closed
   */
  @Test
  public void testTlsVerifyRejectsSelfSigned() throws Exception {
    try (ElasticsearchCatalogOperations operations = catalog(USER, PASSWORD, false)) {
      UncheckedIOException thrown =
          assertThrows(UncheckedIOException.class, () -> operations.listTables(NAMESPACE));

      assertFalse(
          isNameMismatch(thrown),
          "the test certificate is misconfigured, not untrusted: the handshake failed on the "
              + "subject name, so the trust anchor was never consulted and this test proves "
              + "nothing about certificate verification. Chain: "
              + describe(thrown));
      assertTrue(
          isCertificateFailure(thrown),
          "expected a certificate validation failure, got " + describe(thrown));
    }
  }

  /**
   * A catalog carrying no credential fails outright rather than hanging or reporting an empty
   * cluster, and says why in the message the caller reads first.
   *
   * @throws Exception if the catalog cannot be closed
   */
  @Test
  public void testMissingCredentialsFails() throws Exception {
    try (ElasticsearchCatalogOperations operations = catalog(null, null, true)) {
      UncheckedIOException thrown =
          assertThrows(UncheckedIOException.class, () -> operations.listTables(NAMESPACE));

      String message = thrown.getMessage();
      assertTrue(
          message.startsWith("Failed to list Elasticsearch indices"),
          "the operation being attempted was lost from " + message);
      assertTrue(message.contains("401"), "expected an unauthorized status in " + message);
      assertTrue(
          message.contains("missing authentication credentials"),
          "expected a missing credential reason in " + message);
      assertNotNull(thrown.getCause(), "the transport failure was dropped");
    }
  }

  /**
   * A wrong password fails differently from no password at all, so an operator reading only the top
   * level message can tell a misconfigured credential from an absent one.
   *
   * @throws Exception if the catalog cannot be closed
   */
  @Test
  public void testWrongPasswordFails() throws Exception {
    try (ElasticsearchCatalogOperations operations = catalog(USER, "not-the-password", true)) {
      UncheckedIOException thrown =
          assertThrows(UncheckedIOException.class, () -> operations.listTables(NAMESPACE));

      String message = thrown.getMessage();
      assertTrue(message.contains("401"), "expected an unauthorized status in " + message);
      assertTrue(
          message.contains("unable to authenticate user [" + USER + "]"),
          "expected a rejected credential reason in " + message);
      assertFalse(
          message.contains("missing authentication credentials"),
          "a wrong password was reported as a missing one: " + message);
    }
  }

  /**
   * A catalog pointed at the container, with the given credential or none at all.
   *
   * @param skipVerify whether the cluster's untrusted certificate is accepted
   */
  private static ElasticsearchCatalogOperations catalog(
      String username, String password, boolean skipVerify) {
    ImmutableMap.Builder<String, String> conf = ImmutableMap.builder();
    conf.put(ElasticsearchCatalogPropertiesMetadata.HOSTS, uri);
    conf.put(ElasticsearchCatalogPropertiesMetadata.TLS_SKIP_VERIFY, Boolean.toString(skipVerify));
    if (username != null) {
      conf.put(ElasticsearchCatalogPropertiesMetadata.USERNAME, username);
      conf.put(ElasticsearchCatalogPropertiesMetadata.PASSWORD, password);
    }

    ElasticsearchCatalogOperations operations = new ElasticsearchCatalogOperations();
    operations.initialize(conf.build(), null, null);
    return operations;
  }

  /**
   * Reports whether the failure was the certificate being rejected, rather than any other transport
   * error that would also surface as an {@link UncheckedIOException}.
   */
  private static boolean isCertificateFailure(Throwable thrown) {
    for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
      if (cause instanceof SSLHandshakeException
          || cause instanceof CertPathValidatorException
          || cause instanceof CertificateException) {
        return true;
      }
      if (cause.getCause() == cause) {
        break;
      }
    }
    return false;
  }

  /**
   * Reports whether the handshake failed because the certificate's subject did not cover the host
   * that was dialed, rather than because its issuer was untrusted.
   *
   * <p>Both conditions surface as the same exception types, so only the message separates them. A
   * mismatch means the fixture is wrong: the trust check never ran, and a test that accepted it
   * would be green for the wrong reason.
   */
  private static boolean isNameMismatch(Throwable thrown) {
    for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
      String message = cause.getMessage();
      if (message != null) {
        String lower = message.toLowerCase(Locale.ROOT);
        if (lower.contains("no subject alternative")
            || lower.contains("subject alternative names")
            || lower.contains("doesn't match")
            || lower.contains("does not match")
            || lower.contains("hostname verification")
            || lower.contains("no name matching")) {
          return true;
        }
      }
      if (cause.getCause() == cause) {
        break;
      }
    }
    return false;
  }

  /** Renders a cause chain, so a failed expectation says what actually happened. */
  private static String describe(Throwable thrown) {
    StringBuilder chain = new StringBuilder();
    for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
      if (chain.length() > 0) {
        chain.append(" <- ");
      }
      chain.append(cause.getClass().getName()).append(": ").append(cause.getMessage());
      if (cause.getCause() == cause) {
        break;
      }
    }
    return chain.toString();
  }

  /**
   * Generates a self signed certificate inside a throwaway container of the same image, then copies
   * the PEM pair out. Running the cluster's own tool keeps the certificate in the exact shape
   * Elasticsearch expects.
   */
  private static void generateCertificate(Path certificate, Path key) throws Exception {
    try (GenericContainer<?> generator =
        new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withCommand("bash", "-c", GENERATE_CERT)
            .waitingFor(
                Wait.forLogMessage(".*CERT_READY.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(3)))) {
      generator.start();

      generator.copyFileFromContainer(
          GENERATED_DIR + ".crt", certificate.toAbsolutePath().toString());
      generator.copyFileFromContainer(GENERATED_DIR + ".key", key.toAbsolutePath().toString());
    }

    assertTrue(Files.size(certificate) > 0, "the generated certificate is empty");
    assertTrue(Files.size(key) > 0, "the generated key is empty");
  }

  /** Creates the fixture index as the superuser, through the client the connector itself uses. */
  private static void createFixtureIndex() throws Exception {
    CredentialsProvider credentials = new BasicCredentialsProvider();
    credentials.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(USER, PASSWORD));
    SSLContext trustAll =
        new SSLContextBuilder().loadTrustMaterial(null, (chain, authType) -> true).build();

    try (RestClient client =
        RestClient.builder(HttpHost.create(uri))
            .setHttpClientConfigCallback(
                builder ->
                    builder
                        .setDefaultCredentialsProvider(credentials)
                        .setSSLContext(trustAll)
                        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE))
            .build()) {
      Request request = new Request("PUT", "/" + INDEX);
      request.setJsonEntity(MAPPING);

      Response response = client.performRequest(request);
      assertEquals(200, response.getStatusLine().getStatusCode());
    }
  }
}
