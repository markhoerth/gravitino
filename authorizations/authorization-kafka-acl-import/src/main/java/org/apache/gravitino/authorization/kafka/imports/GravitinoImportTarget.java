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

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.authorization.SecurableObject;

/** Every Gravitino call the import tool makes, so tests can substitute a fake. */
public interface GravitinoImportTarget extends Closeable {

  /**
   * @return the properties of the target catalog
   */
  Map<String, String> catalogProperties();

  /**
   * @return the topic names of the catalog's {@code default} schema
   */
  Set<String> listTopics();

  /**
   * @param user the Gravitino user name
   * @return whether the user exists in the metalake
   */
  boolean userExists(String user);

  /**
   * Adds a user. A user that already exists is not an error.
   *
   * @param user the Gravitino user name
   */
  void addUser(String user);

  /**
   * @param role the role name
   * @return whether the role exists in the metalake
   */
  boolean roleExists(String role);

  /**
   * Creates a role.
   *
   * @param role the role name
   * @param securableObjects the securable objects and privileges of the role
   */
  void createRole(String role, List<SecurableObject> securableObjects);

  /**
   * Grants a role to a user.
   *
   * @param role the role name
   * @param user the Gravitino user name
   */
  void grantRoleToUser(String role, String user);
}
