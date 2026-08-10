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
package org.apache.gravitino.catalog.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

/**
 * A thin wrapper over the Elasticsearch low level REST client, exposing only the four metadata
 * calls this catalog makes.
 *
 * <p>The low level client is deliberate. It is an HTTP transport with no server side types, so it
 * talks to 8.x and 9.x clusters alike and keeps the dependency tree free of Elastic licensed
 * artifacts. Responses are parsed with Jackson.
 */
public class ElasticsearchClient implements Closeable {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String MAPPINGS = "mappings";
  private static final String SETTINGS = "settings";
  private static final String INDEX = "index";

  private final RestClient restClient;

  /**
   * Builds a client from catalog properties.
   *
   * @param conf the catalog properties, already validated by {@link
   *     ElasticsearchCatalogPropertiesMetadata#validate(Map)}
   */
  public ElasticsearchClient(Map<String, String> conf) {
    this.restClient = buildRestClient(conf);
  }

  /**
   * Issues {@code GET /}, which reports the cluster name and version.
   *
   * @return the root response
   * @throws IOException if the cluster is unreachable or answers with an error
   */
  public JsonNode ping() throws IOException {
    return get("/");
  }

  /**
   * Fetches an index's mapping.
   *
   * @param index the index name
   * @return the {@code mappings} node, which is empty when the index declares no fields
   * @throws IOException if the cluster is unreachable or answers with an error, including a 404
   *     when the index does not exist
   */
  public JsonNode getMapping(String index) throws IOException {
    JsonNode response = get("/" + encode(index) + "/_mapping");
    JsonNode entry = firstValue(response);
    return entry == null ? MAPPER.createObjectNode() : nodeOrEmpty(entry.get(MAPPINGS));
  }

  /**
   * Fetches an index's settings.
   *
   * @param index the index name
   * @return the {@code settings.index} node, which holds uuid, shard counts and creation date
   * @throws IOException if the cluster is unreachable or answers with an error
   */
  public JsonNode getSettings(String index) throws IOException {
    JsonNode response = get("/" + encode(index) + "/_settings");
    JsonNode entry = firstValue(response);
    if (entry == null) {
      return MAPPER.createObjectNode();
    }
    JsonNode settings = entry.get(SETTINGS);
    return settings == null ? MAPPER.createObjectNode() : nodeOrEmpty(settings.get(INDEX));
  }

  /**
   * Lists indices with the handful of columns this catalog reports as table properties.
   *
   * @return one row per index, in whatever order the cluster returned them
   * @throws IOException if the cluster is unreachable or answers with an error
   */
  public List<JsonNode> catIndices() throws IOException {
    JsonNode rows = get("/_cat/indices?format=json&h=index,status,health,docs.count,store.size");
    if (!rows.isArray()) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<JsonNode> indices = ImmutableList.builder();
    for (JsonNode row : rows) {
      indices.add(row);
    }
    return indices.build();
  }

  @Override
  public void close() throws IOException {
    restClient.close();
  }

  private JsonNode get(String endpoint) throws IOException {
    Response response = restClient.performRequest(new Request("GET", endpoint));
    try (InputStream body = response.getEntity().getContent()) {
      return MAPPER.readTree(body);
    }
  }

  /**
   * Returns the first value of a single entry response. The mapping and settings APIs key their
   * response by the resolved index name, which need not equal the name that was asked for.
   */
  @Nullable
  private static JsonNode firstValue(JsonNode response) {
    if (response == null || !response.isObject() || response.isEmpty()) {
      return null;
    }
    return response.fields().next().getValue();
  }

  private static JsonNode nodeOrEmpty(@Nullable JsonNode node) {
    return node == null ? MAPPER.createObjectNode() : node;
  }

  /** Index names may contain characters that are not safe in a path segment, such as {@code +}. */
  private static String encode(String index) {
    return index.replace("%", "%25").replace("+", "%2B").replace(" ", "%20").replace("#", "%23");
  }

  private static RestClient buildRestClient(Map<String, String> conf) {
    List<String> hosts = ElasticsearchCatalogPropertiesMetadata.hosts(conf);
    HttpHost[] httpHosts = new HttpHost[hosts.size()];
    for (int i = 0; i < hosts.size(); i++) {
      httpHosts[i] = HttpHost.create(hosts.get(i));
    }

    RestClientBuilder builder = RestClient.builder(httpHosts);

    int connectTimeout =
        ElasticsearchCatalogPropertiesMetadata.intOrDefault(
            conf,
            ElasticsearchCatalogPropertiesMetadata.CONNECT_TIMEOUT_MS,
            ElasticsearchCatalogPropertiesMetadata.DEFAULT_CONNECT_TIMEOUT_MS);
    int socketTimeout =
        ElasticsearchCatalogPropertiesMetadata.intOrDefault(
            conf,
            ElasticsearchCatalogPropertiesMetadata.SOCKET_TIMEOUT_MS,
            ElasticsearchCatalogPropertiesMetadata.DEFAULT_SOCKET_TIMEOUT_MS);
    builder.setRequestConfigCallback(
        config -> config.setConnectTimeout(connectTimeout).setSocketTimeout(socketTimeout));

    String apiKey = trimmed(conf.get(ElasticsearchCatalogPropertiesMetadata.API_KEY));
    if (apiKey != null) {
      builder.setDefaultHeaders(
          new Header[] {new BasicHeader("Authorization", "ApiKey " + apiKey)});
    }

    String username = trimmed(conf.get(ElasticsearchCatalogPropertiesMetadata.USERNAME));
    String password = conf.get(ElasticsearchCatalogPropertiesMetadata.PASSWORD);
    boolean skipVerify =
        ElasticsearchCatalogPropertiesMetadata.booleanOrDefault(
            conf, ElasticsearchCatalogPropertiesMetadata.TLS_SKIP_VERIFY, false);

    builder.setHttpClientConfigCallback(
        httpClientBuilder -> {
          if (username != null) {
            CredentialsProvider credentials = new BasicCredentialsProvider();
            credentials.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            httpClientBuilder.setDefaultCredentialsProvider(credentials);
          }

          if (skipVerify) {
            httpClientBuilder
                .setSSLContext(trustAllContext())
                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
          }

          return httpClientBuilder;
        });

    return builder.build();
  }

  /**
   * Builds an SSL context that accepts any certificate. Only reachable when {@code
   * elasticsearch.tls.skip-verify} is explicitly turned on, which exists for clusters presenting a
   * self signed certificate.
   */
  private static SSLContext trustAllContext() {
    try {
      return new SSLContextBuilder().loadTrustMaterial(null, (chain, authType) -> true).build();
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Failed to build an SSL context that skips verification", e);
    }
  }

  @Nullable
  private static String trimmed(@Nullable String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }
}
