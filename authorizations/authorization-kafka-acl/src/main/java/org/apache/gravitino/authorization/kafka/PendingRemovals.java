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

import com.google.common.collect.ImmutableSet;
import java.util.Set;

/**
 * Entities whose removal triggered a hook but may not yet be removed from the Gravitino store (for
 * example, {@code onRoleDeleted} fires before the role is deleted). The grant source treats them as
 * already gone.
 */
public final class PendingRemovals {

  private static final PendingRemovals NONE =
      new PendingRemovals(
          ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), false);

  private final Set<String> roles;
  private final Set<String> users;
  private final Set<String> groups;
  private final Set<String> topics;
  private final boolean catalog;

  private PendingRemovals(
      Set<String> roles,
      Set<String> users,
      Set<String> groups,
      Set<String> topics,
      boolean catalog) {
    this.roles = roles;
    this.users = users;
    this.groups = groups;
    this.topics = topics;
    this.catalog = catalog;
  }

  /**
   * @return no pending removals
   */
  public static PendingRemovals none() {
    return NONE;
  }

  /**
   * @param role the role name
   * @return a pending removal of the role
   */
  public static PendingRemovals role(String role) {
    return new PendingRemovals(
        ImmutableSet.of(role), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), false);
  }

  /**
   * @param user the Gravitino user name
   * @return a pending removal of the user
   */
  public static PendingRemovals user(String user) {
    return new PendingRemovals(
        ImmutableSet.of(), ImmutableSet.of(user), ImmutableSet.of(), ImmutableSet.of(), false);
  }

  /**
   * @param group the Gravitino group name
   * @return a pending removal of the group
   */
  public static PendingRemovals group(String group) {
    return new PendingRemovals(
        ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(group), ImmutableSet.of(), false);
  }

  /**
   * @param topics the Gravitino full names of the topics
   * @return a pending removal of the topics
   */
  public static PendingRemovals topics(Set<String> topics) {
    return new PendingRemovals(
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.copyOf(topics),
        false);
  }

  /**
   * @return a pending removal of the catalog this plugin serves; no grant applies any more
   */
  public static PendingRemovals catalog() {
    return new PendingRemovals(
        ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), true);
  }

  /**
   * @return whether the catalog this plugin serves is being removed
   */
  public boolean removesCatalog() {
    return catalog;
  }

  /**
   * @param role the role name
   * @return whether the role is being removed
   */
  public boolean removesRole(String role) {
    return roles.contains(role);
  }

  /**
   * @param user the Gravitino user name
   * @return whether the user is being removed
   */
  public boolean removesUser(String user) {
    return users.contains(user);
  }

  /**
   * @param group the Gravitino group name
   * @return whether the group is being removed
   */
  public boolean removesGroup(String group) {
    return groups.contains(group);
  }

  /**
   * @param topicFullName the Gravitino full name of the topic
   * @return whether the topic is being removed
   */
  public boolean removesTopic(String topicFullName) {
    return topics.contains(topicFullName);
  }
}
