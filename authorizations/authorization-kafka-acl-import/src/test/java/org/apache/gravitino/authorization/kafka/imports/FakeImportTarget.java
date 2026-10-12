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
package org.apache.gravitino.authorization.kafka.imports;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.authorization.SecurableObject;

/** An in-memory Gravitino that records every call. */
class FakeImportTarget implements GravitinoImportTarget {

  final Map<String, String> catalogProperties;
  final Set<String> topics;
  final Set<String> users = new HashSet<>();
  final Map<String, List<SecurableObject>> roles = new LinkedHashMap<>();
  final Map<String, Set<String>> grants = new HashMap<>();
  final List<String> calls = new ArrayList<>();
  final List<String> writes = new ArrayList<>();
  String failOn;

  FakeImportTarget(Map<String, String> catalogProperties, Set<String> topics) {
    this.catalogProperties = new HashMap<>(catalogProperties);
    this.topics = topics;
  }

  @Override
  public Map<String, String> catalogProperties() {
    calls.add("catalogProperties");
    return catalogProperties;
  }

  @Override
  public Set<String> listTopics() {
    calls.add("listTopics");
    return topics;
  }

  @Override
  public boolean userExists(String user) {
    call("userExists " + user, false);
    return users.contains(user);
  }

  @Override
  public void addUser(String user) {
    call("addUser " + user, true);
    users.add(user);
  }

  @Override
  public Optional<List<SecurableObject>> roleSecurableObjects(String role) {
    call("roleSecurableObjects " + role, false);
    return Optional.ofNullable(roles.get(role));
  }

  @Override
  public void createRole(String role, List<SecurableObject> securableObjects) {
    call("createRole " + role, true);
    roles.put(role, securableObjects);
  }

  @Override
  public void grantRoleToUser(String role, String user) {
    call("grantRoleToUser " + role + " " + user, true);
    grants.computeIfAbsent(user, u -> new HashSet<>()).add(role);
  }

  @Override
  public void setCatalogProperty(String key, String value) {
    call("setCatalogProperty " + key + "=" + value, true);
    catalogProperties.put(key, value);
  }

  @Override
  public String triggerReconcile() {
    call("triggerReconcile", true);
    return "fake";
  }

  @Override
  public void close() {}

  /** Records a call, then throws if {@link #failOn} is a prefix of it. */
  private void call(String call, boolean write) {
    calls.add(call);
    if (failOn != null && call.startsWith(failOn)) {
      throw new IllegalStateException("injected failure: " + call);
    }
    if (write) {
      writes.add(call);
    }
  }
}
