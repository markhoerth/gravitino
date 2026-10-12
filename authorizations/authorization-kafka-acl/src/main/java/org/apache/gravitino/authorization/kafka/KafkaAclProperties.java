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
package org.apache.gravitino.authorization.kafka;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.authorization.common.AuthorizationProperties;
import org.apache.kafka.clients.admin.AdminClientConfig;

/** Properties of the Kafka ACL authorization plugin, read from the catalog configuration. */
public class KafkaAclProperties extends AuthorizationProperties {

  /** The prefix shared by all Kafka ACL authorization properties. */
  public static final String PREFIX = "authorization.kafka.";

  /**
   * Only Principals starting with this prefix are pushed down, and only ACLs whose Principal starts
   * with it are ever modified. This is the ownership rule of the plugin.
   */
  public static final String PRINCIPAL_PREFIX = PREFIX + "principal-prefix";

  /** The default principal prefix. */
  public static final String DEFAULT_PRINCIPAL_PREFIX = "User:flink.";

  /**
   * Template for the PREFIXED consumer group pattern granted to each Principal with a consume
   * grant. Supports the {@code ${metalake}} and {@code ${principal}} placeholders.
   */
  public static final String GROUP_PREFIX_TEMPLATE = PREFIX + "group-prefix-template";

  /** The default consumer group prefix template. */
  public static final String DEFAULT_GROUP_PREFIX_TEMPLATE = "flink.${metalake}.${principal}.";

  /** Placeholder for the metalake name in the group prefix template. */
  public static final String METALAKE_PLACEHOLDER = "${metalake}";

  /** Placeholder for the Principal name (without the type) in the group prefix template. */
  public static final String PRINCIPAL_PLACEHOLDER = "${principal}";

  /** Properties with this prefix are passed through, prefix stripped, to the Kafka AdminClient. */
  public static final String ADMIN_PREFIX = PREFIX + "admin.";

  /** The Kafka catalog property holding the bootstrap servers. */
  public static final String BOOTSTRAP_SERVERS = AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

  private static final String[] SENSITIVE_SUFFIXES = {"password", "secret", "jaas.config"};

  private final String bootstrapServers;

  /**
   * Creates the properties from the catalog configuration.
   *
   * @param properties the catalog configuration passed to the plugin
   */
  public KafkaAclProperties(Map<String, String> properties) {
    super(properties);
    this.bootstrapServers = properties.get(BOOTSTRAP_SERVERS);
  }

  /**
   * Returns whether a property holds a secret and must never be logged.
   *
   * @param key the property key
   * @return true if the key ends in password, secret or jaas.config
   */
  public static boolean isSensitive(String key) {
    String lower = key.toLowerCase(Locale.ROOT);
    for (String suffix : SENSITIVE_SUFFIXES) {
      if (lower.endsWith(suffix)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String getPropertiesPrefix() {
    return PREFIX;
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(
        bootstrapServers != null && !bootstrapServers.trim().isEmpty(),
        "Kafka ACL authorization requires the catalog property %s",
        BOOTSTRAP_SERVERS);
    String principalPrefix = principalPrefix();
    Preconditions.checkArgument(
        principalPrefix.indexOf(':') > 0 && !principalPrefix.endsWith(":"),
        "%s must contain a Principal type and a non-empty name prefix, e.g. %s, but was '%s'",
        PRINCIPAL_PREFIX,
        DEFAULT_PRINCIPAL_PREFIX,
        principalPrefix);
    Preconditions.checkArgument(
        groupPrefixTemplate().contains(PRINCIPAL_PLACEHOLDER),
        "%s must contain %s so that each Principal gets its own consumer group prefix",
        GROUP_PREFIX_TEMPLATE,
        PRINCIPAL_PLACEHOLDER);
  }

  /**
   * Returns the principal prefix.
   *
   * @return the configured principal prefix or {@link #DEFAULT_PRINCIPAL_PREFIX}
   */
  public String principalPrefix() {
    return properties.getOrDefault(PRINCIPAL_PREFIX, DEFAULT_PRINCIPAL_PREFIX);
  }

  /**
   * Returns the consumer group prefix template.
   *
   * @return the configured template or {@link #DEFAULT_GROUP_PREFIX_TEMPLATE}
   */
  public String groupPrefixTemplate() {
    return properties.getOrDefault(GROUP_PREFIX_TEMPLATE, DEFAULT_GROUP_PREFIX_TEMPLATE);
  }

  /**
   * Returns the configuration for the Kafka AdminClient: the catalog bootstrap servers plus every
   * {@code authorization.kafka.admin.*} property with the prefix stripped.
   *
   * @return the AdminClient configuration
   */
  public Map<String, Object> adminClientConfig() {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put(BOOTSTRAP_SERVERS, bootstrapServers);
    properties.entrySet().stream()
        .filter(e -> e.getKey().startsWith(ADMIN_PREFIX))
        .filter(e -> !e.getKey().substring(ADMIN_PREFIX.length()).equals(BOOTSTRAP_SERVERS))
        .forEach(e -> builder.put(e.getKey().substring(ADMIN_PREFIX.length()), e.getValue()));
    return builder.build();
  }

  @Override
  public String toString() {
    return properties.entrySet().stream()
        .map(e -> e.getKey() + "=" + (isSensitive(e.getKey()) ? "******" : e.getValue()))
        .sorted()
        .collect(Collectors.joining(", ", "KafkaAclProperties{", "}"));
  }
}
