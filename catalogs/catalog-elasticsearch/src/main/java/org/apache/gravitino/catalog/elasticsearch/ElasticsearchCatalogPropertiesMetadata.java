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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/**
 * Catalog level properties for the Elasticsearch catalog.
 *
 * <p>Note this extends {@link BaseCatalogPropertiesMetadata} rather than {@code
 * BasePropertiesMetadata}. The base catalog class contributes the framework's own entries, and
 * without them catalog creation fails with a misleading complaint about {@code
 * authorization-provider} not being defined.
 */
public class ElasticsearchCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  /** Comma separated cluster endpoints, each carrying an explicit scheme. */
  public static final String HOSTS = "elasticsearch.hosts";

  /** Base64 encoded {@code id:key}, sent as an {@code Authorization: ApiKey} header. */
  public static final String API_KEY = "elasticsearch.api-key";

  /** Basic auth user, mutually exclusive with {@link #API_KEY}. */
  public static final String USERNAME = "elasticsearch.username";

  /** Basic auth password. */
  public static final String PASSWORD = "elasticsearch.password";

  /** Connect timeout in milliseconds. */
  public static final String CONNECT_TIMEOUT_MS = "elasticsearch.connect-timeout-ms";

  /** Socket timeout in milliseconds. */
  public static final String SOCKET_TIMEOUT_MS = "elasticsearch.socket-timeout-ms";

  /** Whether indices whose name starts with a dot are surfaced as tables. */
  public static final String INCLUDE_SYSTEM_INDICES = "elasticsearch.include-system-indices";

  /** Whether TLS certificate and hostname verification is skipped. */
  public static final String TLS_SKIP_VERIFY = "elasticsearch.tls.skip-verify";

  /** Default connect timeout in milliseconds. */
  public static final int DEFAULT_CONNECT_TIMEOUT_MS = 5000;

  /** Default socket timeout in milliseconds. */
  public static final int DEFAULT_SOCKET_TIMEOUT_MS = 30000;

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              HOSTS,
              PropertyEntry.stringRequiredPropertyEntry(
                  HOSTS,
                  "Comma separated Elasticsearch endpoints, each with an explicit scheme, "
                      + "for example https://es.example.svc:9200.",
                  false,
                  false))
          .put(
              API_KEY,
              PropertyEntry.stringOptionalPropertyEntry(
                  API_KEY,
                  "Base64 encoded id:key, sent as an Authorization: ApiKey header. "
                      + "Mutually exclusive with "
                      + USERNAME
                      + ".",
                  false,
                  null,
                  true))
          .put(
              USERNAME,
              PropertyEntry.stringOptionalPropertyEntry(
                  USERNAME,
                  "Basic auth user. Mutually exclusive with " + API_KEY + ".",
                  false,
                  null,
                  false))
          .put(
              PASSWORD,
              PropertyEntry.stringOptionalPropertyEntry(
                  PASSWORD, "Basic auth password.", false, null, true))
          .put(
              CONNECT_TIMEOUT_MS,
              PropertyEntry.integerOptionalPropertyEntry(
                  CONNECT_TIMEOUT_MS,
                  "Connect timeout in milliseconds.",
                  false,
                  DEFAULT_CONNECT_TIMEOUT_MS,
                  false))
          .put(
              SOCKET_TIMEOUT_MS,
              PropertyEntry.integerOptionalPropertyEntry(
                  SOCKET_TIMEOUT_MS,
                  "Socket timeout in milliseconds.",
                  false,
                  DEFAULT_SOCKET_TIMEOUT_MS,
                  false))
          .put(
              INCLUDE_SYSTEM_INDICES,
              PropertyEntry.booleanPropertyEntry(
                  INCLUDE_SYSTEM_INDICES,
                  "Whether indices whose name starts with a dot are surfaced as tables.",
                  false,
                  false,
                  false,
                  false,
                  false))
          .put(
              TLS_SKIP_VERIFY,
              PropertyEntry.booleanPropertyEntry(
                  TLS_SKIP_VERIFY,
                  "Whether TLS certificate and hostname verification is skipped. Intended for "
                      + "clusters using a self signed certificate.",
                  false,
                  false,
                  false,
                  false,
                  false))
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  /**
   * Rejects property combinations that would otherwise fail later as an opaque connection error.
   *
   * @param conf the catalog properties
   * @throws IllegalArgumentException if hosts are missing or unschemed, or if both authentication
   *     mechanisms are configured
   */
  public static void validate(Map<String, String> conf) {
    List<String> hosts = hosts(conf);
    if (hosts.isEmpty()) {
      throw new IllegalArgumentException(HOSTS + " is required for an Elasticsearch catalog");
    }

    for (String host : hosts) {
      if (!host.startsWith("http://") && !host.startsWith("https://")) {
        throw new IllegalArgumentException(
            HOSTS
                + " entry "
                + host
                + " must carry a scheme, for example https://es.example.svc:9200");
      }
    }

    if (isSet(conf, API_KEY) && isSet(conf, USERNAME)) {
      throw new IllegalArgumentException(
          API_KEY + " and " + USERNAME + " are mutually exclusive; configure exactly one");
    }
  }

  /**
   * Splits the configured endpoints, dropping blank entries left by a trailing comma.
   *
   * @param conf the catalog properties
   * @return the endpoints in the order they were configured
   */
  public static List<String> hosts(Map<String, String> conf) {
    String raw = conf.get(HOSTS);
    if (raw == null) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<String> hosts = ImmutableList.builder();
    for (String host : raw.split(",")) {
      String trimmed = host.trim();
      if (!trimmed.isEmpty()) {
        hosts.add(trimmed);
      }
    }
    return hosts.build();
  }

  /**
   * Reads an integer property, falling back to a default when it is absent or blank.
   *
   * @param conf the catalog properties
   * @param key the property name
   * @param defaultValue the value to use when the property is not set
   * @return the configured value, or the default
   */
  public static int intOrDefault(Map<String, String> conf, String key, int defaultValue) {
    String value = conf.get(key);
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    return Integer.parseInt(value.trim());
  }

  /**
   * Reads a boolean property, falling back to a default when it is absent or blank.
   *
   * @param conf the catalog properties
   * @param key the property name
   * @param defaultValue the value to use when the property is not set
   * @return the configured value, or the default
   */
  public static boolean booleanOrDefault(
      Map<String, String> conf, String key, boolean defaultValue) {
    String value = conf.get(key);
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    return Boolean.parseBoolean(value.trim());
  }

  private static boolean isSet(Map<String, String> conf, String key) {
    String value = conf.get(key);
    return value != null && !value.trim().isEmpty();
  }
}
