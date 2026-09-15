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
package org.apache.gravitino.authorization.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.gravitino.exceptions.AuthorizationPluginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A thin client over the subset of the Elasticsearch security API the authorization plugin needs:
 * roles, role mappings, API key invalidation, index listing and license level.
 */
public class ElasticsearchSecurityClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSecurityClient.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String baseUrl;
  private final String authorizationHeader;
  private final Duration requestTimeout;
  private final HttpClient httpClient;

  /**
   * Creates a client from plugin properties.
   *
   * @param properties the validated plugin properties.
   */
  public ElasticsearchSecurityClient(ElasticsearchAuthorizationProperties properties) {
    this.baseUrl = properties.url();
    this.requestTimeout = Duration.ofMillis(properties.requestTimeoutMs());
    this.authorizationHeader = buildAuthorizationHeader(properties);

    HttpClient.Builder builder =
        HttpClient.newBuilder()
            .connectTimeout(requestTimeout)
            .followRedirects(HttpClient.Redirect.NORMAL);
    if (properties.tlsInsecure()) {
      LOG.warn(
          "TLS certificate verification is disabled for {}. Do not use this outside development.",
          baseUrl);
      builder.sslContext(insecureSslContext());
    }
    this.httpClient = builder.build();
  }

  @VisibleForTesting
  ElasticsearchSecurityClient(
      String baseUrl, String authorizationHeader, Duration requestTimeout, HttpClient httpClient) {
    this.baseUrl = baseUrl;
    this.authorizationHeader = authorizationHeader;
    this.requestTimeout = requestTimeout;
    this.httpClient = httpClient;
  }

  /**
   * Creates or replaces an Elasticsearch role.
   *
   * @param roleName the role name.
   * @param roleDescriptor the role descriptor body.
   */
  public void putRole(String roleName, ObjectNode roleDescriptor) {
    execute("PUT", "/_security/role/" + encode(roleName), roleDescriptor.toString());
  }

  /**
   * Deletes an Elasticsearch role. A role that does not exist is not an error.
   *
   * @param roleName the role name.
   */
  public void deleteRole(String roleName) {
    executeAllowingNotFound("DELETE", "/_security/role/" + encode(roleName), null);
  }

  /**
   * Reads an Elasticsearch role.
   *
   * @param roleName the role name.
   * @return the role descriptor, empty when the role does not exist.
   */
  public Optional<JsonNode> getRole(String roleName) {
    String body = executeAllowingNotFound("GET", "/_security/role/" + encode(roleName), null);
    return parseNamedEntry(body, roleName);
  }

  /**
   * Creates or replaces a role mapping.
   *
   * @param mappingName the role mapping name.
   * @param mapping the role mapping body.
   */
  public void putRoleMapping(String mappingName, ObjectNode mapping) {
    execute("PUT", "/_security/role_mapping/" + encode(mappingName), mapping.toString());
  }

  /**
   * Deletes a role mapping. A mapping that does not exist is not an error.
   *
   * @param mappingName the role mapping name.
   */
  public void deleteRoleMapping(String mappingName) {
    executeAllowingNotFound("DELETE", "/_security/role_mapping/" + encode(mappingName), null);
  }

  /**
   * Reads a role mapping.
   *
   * @param mappingName the role mapping name.
   * @return the role mapping body, empty when the mapping does not exist.
   */
  public Optional<JsonNode> getRoleMapping(String mappingName) {
    String body =
        executeAllowingNotFound("GET", "/_security/role_mapping/" + encode(mappingName), null);
    return parseNamedEntry(body, mappingName);
  }

  /**
   * Lists the names of every concrete index on the cluster. Used to expand a pattern into an
   * explicit allow list when a deny sits below an allow.
   *
   * @return the index names.
   */
  public List<String> listIndices() {
    String body = execute("GET", "/_cat/indices?format=json&h=index&expand_wildcards=open", null);
    List<String> indices = new ArrayList<>();
    try {
      JsonNode root = MAPPER.readTree(body);
      for (JsonNode entry : root) {
        JsonNode index = entry.get("index");
        if (index != null && !index.asText().startsWith(".")) {
          indices.add(index.asText());
        }
      }
    } catch (IOException e) {
      throw new AuthorizationPluginException(
          "Failed to parse the Elasticsearch index list: %s", e.getMessage());
    }
    Collections.sort(indices);
    return indices;
  }

  /**
   * Returns the license type reported by the cluster, for example {@code basic}, {@code trial},
   * {@code platinum} or {@code enterprise}. Document- and field-level security require a paid tier.
   *
   * @return the license type in lower case, or {@code unknown} when it cannot be read.
   */
  public String licenseType() {
    try {
      String body = execute("GET", "/_license", null);
      JsonNode license = MAPPER.readTree(body).path("license").path("type");
      return license.isMissingNode() ? "unknown" : license.asText().toLowerCase();
    } catch (IOException | AuthorizationPluginException e) {
      LOG.warn("Failed to read the Elasticsearch license level", e);
      return "unknown";
    }
  }

  /**
   * Invalidates every API key owned by a user, so that a revoked grant reaches keys already issued.
   *
   * @param username the owner's username.
   */
  public void invalidateApiKeysOf(String username) {
    ObjectNode body = MAPPER.createObjectNode();
    body.put("username", username);
    try {
      execute("DELETE", "/_security/api_key", body.toString());
    } catch (AuthorizationPluginException e) {
      // Key invalidation is best effort: the grant change itself has already been applied.
      LOG.warn("Failed to invalidate Elasticsearch API keys for user {}", username, e);
    }
  }

  @Override
  public void close() {
    // java.net.http.HttpClient holds no resources requiring an explicit close.
  }

  private String execute(String method, String path, String body) {
    HttpResponse<String> response = send(method, path, body);
    if (response.statusCode() >= 200 && response.statusCode() < 300) {
      return response.body();
    }
    throw new AuthorizationPluginException(
        "Elasticsearch %s %s failed with status %d: %s",
        method, path, response.statusCode(), response.body());
  }

  private String executeAllowingNotFound(String method, String path, String body) {
    HttpResponse<String> response = send(method, path, body);
    if (response.statusCode() == 404) {
      return null;
    }
    if (response.statusCode() >= 200 && response.statusCode() < 300) {
      return response.body();
    }
    throw new AuthorizationPluginException(
        "Elasticsearch %s %s failed with status %d: %s",
        method, path, response.statusCode(), response.body());
  }

  private HttpResponse<String> send(String method, String path, String body) {
    HttpRequest.BodyPublisher publisher =
        body == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8);

    HttpRequest.Builder builder =
        HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + path))
            .timeout(requestTimeout)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .method(method, publisher);
    if (authorizationHeader != null) {
      builder.header("Authorization", authorizationHeader);
    }

    try {
      return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    } catch (IOException e) {
      throw new AuthorizationPluginException(
          "Failed to call Elasticsearch %s %s: %s", method, path, e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AuthorizationPluginException(
          "Interrupted while calling Elasticsearch %s %s", method, path);
    }
  }

  private Optional<JsonNode> parseNamedEntry(String body, String name) {
    if (body == null) {
      return Optional.empty();
    }
    try {
      JsonNode root = MAPPER.readTree(body);
      JsonNode entry = root.get(name);
      return entry == null ? Optional.empty() : Optional.of(entry);
    } catch (IOException e) {
      throw new AuthorizationPluginException(
          "Failed to parse the Elasticsearch response for %s: %s", name, e.getMessage());
    }
  }

  private static String buildAuthorizationHeader(ElasticsearchAuthorizationProperties properties) {
    if (ElasticsearchAuthorizationProperties.AUTH_TYPE_API_KEY.equals(properties.authType())) {
      return "ApiKey " + properties.apiKey();
    }
    String credentials = properties.username() + ":" + properties.password();
    return "Basic "
        + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static SSLContext insecureSslContext() {
    try {
      TrustManager trustAll =
          new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {}

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {}

            @Override
            public X509Certificate[] getAcceptedIssuers() {
              return new X509Certificate[0];
            }
          };
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(null, new TrustManager[] {trustAll}, new SecureRandom());
      return context;
    } catch (Exception e) {
      throw new AuthorizationPluginException(
          "Failed to build an SSL context that skips certificate verification: %s", e.getMessage());
    }
  }
}
