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

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps Gravitino users and groups to Kafka Principals. Only Principals under the configured
 * principal prefix (workload identities) are pushed down; everything else is skipped.
 */
public class KafkaPrincipalResolver {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaPrincipalResolver.class);

  /** The Kafka Principal type for users. */
  public static final String USER_TYPE = "User:";

  private final String principalPrefix;
  private final Function<String, Collection<String>> groupMembers;

  /**
   * Creates a resolver.
   *
   * @param principalPrefix only Principals starting with this prefix are resolved
   * @param groupMembers returns the Gravitino user names that are members of a Gravitino group
   */
  public KafkaPrincipalResolver(
      String principalPrefix, Function<String, Collection<String>> groupMembers) {
    this.principalPrefix = principalPrefix;
    this.groupMembers = groupMembers;
  }

  /**
   * Creates a resolver that knows no group members. Gravitino groups do not record their members,
   * so group grants are not pushed down unless a membership source is supplied.
   *
   * @param principalPrefix only Principals starting with this prefix are resolved
   * @return the resolver
   */
  public static KafkaPrincipalResolver withoutGroupMembership(String principalPrefix) {
    return new KafkaPrincipalResolver(principalPrefix, group -> Collections.emptyList());
  }

  /**
   * Maps a Gravitino user to its Kafka Principal.
   *
   * @param userName the Gravitino user name
   * @return {@code User:<userName>}, or empty if it does not start with the principal prefix
   */
  public Optional<String> resolveUser(String userName) {
    String principal = USER_TYPE + userName;
    if (!principal.startsWith(principalPrefix)) {
      LOG.debug(
          "Skip user {}: Principal {} is not under prefix {}",
          userName,
          principal,
          principalPrefix);
      return Optional.empty();
    }
    return Optional.of(principal);
  }

  /**
   * Expands a Gravitino group to the Kafka Principals of its member users.
   *
   * @param groupName the Gravitino group name
   * @return the Principals of the members that are under the principal prefix
   */
  public Set<String> resolveGroup(String groupName) {
    Collection<String> members = groupMembers.apply(groupName);
    if (members.isEmpty()) {
      LOG.warn("Group {} has no known members; its grants are not pushed down to Kafka", groupName);
    }
    return members.stream()
        .map(this::resolveUser)
        .flatMap(Optional::stream)
        .collect(Collectors.toSet());
  }

  /**
   * Returns whether a Kafka Principal is owned by this plugin.
   *
   * @param principal the Kafka Principal
   * @return true if it starts with the principal prefix
   */
  public boolean owns(String principal) {
    return principal.startsWith(principalPrefix);
  }

  /**
   * Strips the Principal type, e.g. {@code User:flink.a} becomes {@code flink.a}.
   *
   * @param principal the Kafka Principal
   * @return the Principal name
   */
  public static String principalName(String principal) {
    int idx = principal.indexOf(':');
    return idx < 0 ? principal : principal.substring(idx + 1);
  }
}
