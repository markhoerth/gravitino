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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;

/** A {@link GravitinoImportTarget} backed by the Gravitino Java client with simple auth. */
public class GravitinoClientImportTarget implements GravitinoImportTarget {

  private final GravitinoClient client;
  private final String catalog;

  /**
   * Connects to Gravitino.
   *
   * @param uri the Gravitino server URI
   * @param metalake the metalake name
   * @param catalog the Kafka catalog name
   * @param user the user for simple authentication
   */
  public GravitinoClientImportTarget(String uri, String metalake, String catalog, String user) {
    this.client = GravitinoClient.builder(uri).withMetalake(metalake).withSimpleAuth(user).build();
    this.catalog = catalog;
  }

  @Override
  public Map<String, String> catalogProperties() {
    Map<String, String> properties = client.loadCatalog(catalog).properties();
    return properties == null ? Collections.emptyMap() : properties;
  }

  @Override
  public Set<String> listTopics() {
    NameIdentifier[] topics =
        client
            .loadCatalog(catalog)
            .asTopicCatalog()
            .listTopics(Namespace.of(KafkaAclImportTool.DEFAULT_SCHEMA));
    return Arrays.stream(topics).map(NameIdentifier::name).collect(Collectors.toSet());
  }

  @Override
  public boolean userExists(String user) {
    try {
      client.getUser(user);
      return true;
    } catch (NoSuchUserException e) {
      return false;
    }
  }

  @Override
  public void addUser(String user) {
    try {
      client.addUser(user);
    } catch (UserAlreadyExistsException e) {
      // Fine: another run or an operator added the user first.
    }
  }

  @Override
  public boolean roleExists(String role) {
    try {
      client.getRole(role);
      return true;
    } catch (NoSuchRoleException e) {
      return false;
    }
  }

  @Override
  public void createRole(String role, List<SecurableObject> securableObjects) {
    client.createRole(role, Collections.emptyMap(), securableObjects);
  }

  @Override
  public void grantRoleToUser(String role, String user) {
    client.grantRolesToUser(Collections.singletonList(role), user);
  }

  @Override
  public void close() {
    client.close();
  }
}
