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

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.authorization.common.AuthorizationProperties;

/**
 * Properties for the Elasticsearch authorization plugin.
 *
 * <p>The plugin pushes Gravitino roles and grants into the Elasticsearch security API, so that
 * clients querying Elasticsearch directly are subject to the privileges defined in Gravitino.
 */
public class ElasticsearchAuthorizationProperties extends AuthorizationProperties {

  /** The prefix every property of this plugin carries. */
  public static final String ELASTICSEARCH_PREFIX = "authorization.elasticsearch";

  /** Base URL of the Elasticsearch cluster, for example {@code https://es.example.com:9200}. */
  public static final String URL = "authorization.elasticsearch.url";

  /** Authentication type used by the plugin itself: {@code basic} or {@code api-key}. */
  public static final String AUTH_TYPE = "authorization.elasticsearch.auth.type";

  /** Value for {@link #AUTH_TYPE} selecting HTTP basic authentication. */
  public static final String AUTH_TYPE_BASIC = "basic";

  /** Value for {@link #AUTH_TYPE} selecting API key authentication. */
  public static final String AUTH_TYPE_API_KEY = "api-key";

  /** Username used when {@link #AUTH_TYPE} is {@code basic}. */
  public static final String USERNAME = "authorization.elasticsearch.username";

  /** Password used when {@link #AUTH_TYPE} is {@code basic}. */
  public static final String PASSWORD = "authorization.elasticsearch.password";

  /** Encoded API key used when {@link #AUTH_TYPE} is {@code api-key}. */
  public static final String API_KEY = "authorization.elasticsearch.api-key";

  /** Prefix applied to every Elasticsearch role and role mapping the plugin owns. */
  public static final String ROLE_PREFIX = "authorization.elasticsearch.role.prefix";

  /** Default value for {@link #ROLE_PREFIX}. */
  public static final String DEFAULT_ROLE_PREFIX = "gravitino_";

  /**
   * Index prefix that scopes catalog and schema level grants. Elasticsearch has a single flat index
   * namespace, so a catalog grant becomes the pattern {@code <prefix>*}. An empty prefix makes a
   * catalog grant cover every index on the cluster.
   */
  public static final String INDEX_PREFIX = "authorization.elasticsearch.index.prefix";

  /** Default value for {@link #INDEX_PREFIX}. */
  public static final String DEFAULT_INDEX_PREFIX = "";

  /**
   * Interval in seconds between reconciliation passes. Reconciliation re-applies every role the
   * plugin owns, which is what picks up indices created after a deny was expanded into an explicit
   * allow list. Set to 0 to disable.
   */
  public static final String RECONCILE_INTERVAL_SECONDS =
      "authorization.elasticsearch.reconcile.interval-seconds";

  /** Default value for {@link #RECONCILE_INTERVAL_SECONDS}. */
  public static final long DEFAULT_RECONCILE_INTERVAL_SECONDS = 300L;

  /** Request timeout in milliseconds for calls to the Elasticsearch security API. */
  public static final String REQUEST_TIMEOUT_MS = "authorization.elasticsearch.request.timeout-ms";

  /** Default value for {@link #REQUEST_TIMEOUT_MS}. */
  public static final long DEFAULT_REQUEST_TIMEOUT_MS = 10_000L;

  /**
   * Skip TLS certificate verification. Intended for development clusters using self-signed
   * certificates.
   */
  public static final String TLS_INSECURE = "authorization.elasticsearch.tls.insecure";

  /**
   * Invalidate the Elasticsearch API keys owned by a user when that user's grants change. Keys
   * capture the owner's privileges at creation time, so without this a revoke does not reach keys
   * already issued.
   */
  public static final String INVALIDATE_API_KEYS_ON_REVOKE =
      "authorization.elasticsearch.api-key.invalidate-on-revoke";

  /**
   * Creates the properties view for this plugin.
   *
   * @param properties the catalog properties.
   */
  public ElasticsearchAuthorizationProperties(Map<String, String> properties) {
    super(properties);
  }

  @Override
  public String getPropertiesPrefix() {
    return ELASTICSEARCH_PREFIX;
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(StringUtils.isNotBlank(properties.get(URL)), "%s is required", URL);

    String authType = authType();
    Preconditions.checkArgument(
        AUTH_TYPE_BASIC.equals(authType) || AUTH_TYPE_API_KEY.equals(authType),
        "%s must be either %s or %s",
        AUTH_TYPE,
        AUTH_TYPE_BASIC,
        AUTH_TYPE_API_KEY);

    if (AUTH_TYPE_BASIC.equals(authType)) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(properties.get(USERNAME)),
          "%s is required when %s is %s",
          USERNAME,
          AUTH_TYPE,
          AUTH_TYPE_BASIC);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(properties.get(PASSWORD)),
          "%s is required when %s is %s",
          PASSWORD,
          AUTH_TYPE,
          AUTH_TYPE_BASIC);
    } else {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(properties.get(API_KEY)),
          "%s is required when %s is %s",
          API_KEY,
          AUTH_TYPE,
          AUTH_TYPE_API_KEY);
    }

    Preconditions.checkArgument(
        reconcileIntervalSeconds() >= 0, "%s must not be negative", RECONCILE_INTERVAL_SECONDS);
    Preconditions.checkArgument(requestTimeoutMs() > 0, "%s must be positive", REQUEST_TIMEOUT_MS);
  }

  /**
   * Returns the cluster URL with any trailing slash removed.
   *
   * @return the cluster URL.
   */
  public String url() {
    String url = properties.get(URL);
    return url == null ? null : StringUtils.stripEnd(url.trim(), "/");
  }

  /**
   * Returns the authentication type the plugin uses to reach Elasticsearch.
   *
   * @return {@code basic} or {@code api-key}.
   */
  public String authType() {
    return properties.getOrDefault(AUTH_TYPE, AUTH_TYPE_BASIC).trim().toLowerCase();
  }

  /**
   * Returns the basic-auth username.
   *
   * @return the username, or null when API key authentication is in use.
   */
  public String username() {
    return properties.get(USERNAME);
  }

  /**
   * Returns the basic-auth password.
   *
   * @return the password, or null when API key authentication is in use.
   */
  public String password() {
    return properties.get(PASSWORD);
  }

  /**
   * Returns the encoded API key.
   *
   * @return the API key, or null when basic authentication is in use.
   */
  public String apiKey() {
    return properties.get(API_KEY);
  }

  /**
   * Returns the prefix applied to every Elasticsearch role this plugin owns.
   *
   * @return the role prefix.
   */
  public String rolePrefix() {
    return properties.getOrDefault(ROLE_PREFIX, DEFAULT_ROLE_PREFIX);
  }

  /**
   * Returns the index prefix that scopes catalog and schema level grants.
   *
   * @return the index prefix, possibly empty.
   */
  public String indexPrefix() {
    return properties.getOrDefault(INDEX_PREFIX, DEFAULT_INDEX_PREFIX);
  }

  /**
   * Returns the reconciliation interval.
   *
   * @return the interval in seconds, or 0 when reconciliation is disabled.
   */
  public long reconcileIntervalSeconds() {
    return longProperty(RECONCILE_INTERVAL_SECONDS, DEFAULT_RECONCILE_INTERVAL_SECONDS);
  }

  /**
   * Returns the request timeout for Elasticsearch security API calls.
   *
   * @return the timeout in milliseconds.
   */
  public long requestTimeoutMs() {
    return longProperty(REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT_MS);
  }

  /**
   * Returns whether TLS certificate verification is skipped.
   *
   * @return true when certificate verification is disabled.
   */
  public boolean tlsInsecure() {
    return Boolean.parseBoolean(properties.getOrDefault(TLS_INSECURE, "false"));
  }

  /**
   * Returns whether a user's API keys are invalidated when that user's grants change.
   *
   * @return true when outstanding API keys are invalidated on revoke.
   */
  public boolean invalidateApiKeysOnRevoke() {
    return Boolean.parseBoolean(properties.getOrDefault(INVALIDATE_API_KEYS_ON_REVOKE, "true"));
  }

  private long longProperty(String key, long defaultValue) {
    String value = properties.get(key);
    if (StringUtils.isBlank(value)) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be a number, got: " + value, e);
    }
  }
}
