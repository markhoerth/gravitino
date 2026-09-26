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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Set;
import org.apache.gravitino.authorization.Privilege;

/**
 * Translation of Gravitino privileges into Elasticsearch index privileges.
 *
 * <p>Elasticsearch privileges are additive and carry no hierarchy, so a Gravitino privilege becomes
 * a set of index privileges attached to an index name or pattern. Privileges with no Elasticsearch
 * counterpart translate to the empty set and are dropped.
 */
public final class ElasticsearchPrivileges {

  /** Elasticsearch privilege granting read access to documents in an index. */
  public static final String READ = "read";

  /** Elasticsearch privilege granting index, update and delete of documents. */
  public static final String WRITE = "write";

  /** Elasticsearch privilege granting visibility of an index and its mapping. */
  public static final String VIEW_INDEX_METADATA = "view_index_metadata";

  /** Elasticsearch privilege granting index creation. */
  public static final String CREATE_INDEX = "create_index";

  /** Elasticsearch privilege granting index deletion. */
  public static final String DELETE_INDEX = "delete_index";

  private static final ImmutableMap<Privilege.Name, Set<String>> MAPPING =
      ImmutableMap.<Privilege.Name, Set<String>>builder()
          // Reading data from an index implies being able to see the index exists.
          .put(Privilege.Name.SELECT_TABLE, ImmutableSet.of(READ, VIEW_INDEX_METADATA))
          .put(Privilege.Name.SELECT_VIEW, ImmutableSet.of(READ, VIEW_INDEX_METADATA))
          // Writing documents or changing the mapping.
          .put(Privilege.Name.MODIFY_TABLE, ImmutableSet.of(WRITE, VIEW_INDEX_METADATA))
          // Index lifecycle.
          .put(Privilege.Name.CREATE_TABLE, ImmutableSet.of(CREATE_INDEX))
          .put(Privilege.Name.CREATE_VIEW, ImmutableSet.of(CREATE_INDEX))
          // Discovery only: the index is visible but its documents are not readable.
          .put(Privilege.Name.USE_CATALOG, ImmutableSet.of(VIEW_INDEX_METADATA))
          .put(Privilege.Name.USE_SCHEMA, ImmutableSet.of(VIEW_INDEX_METADATA))
          .build();

  private static final ImmutableSet<Privilege.Name> USAGE_GATED =
      ImmutableSet.of(
          Privilege.Name.SELECT_TABLE,
          Privilege.Name.SELECT_VIEW,
          Privilege.Name.MODIFY_TABLE,
          Privilege.Name.CREATE_TABLE,
          Privilege.Name.CREATE_VIEW);

  private ElasticsearchPrivileges() {}

  /**
   * Returns whether a privilege reaches data or objects, and so requires the usage privileges
   * alongside it. Gravitino gates these on USE_CATALOG and USE_SCHEMA; USE_CATALOG and USE_SCHEMA
   * themselves are not gated.
   *
   * @param name the Gravitino privilege name.
   * @return true when the privilege requires USE_CATALOG and USE_SCHEMA.
   */
  public static boolean requiresUsage(Privilege.Name name) {
    return USAGE_GATED.contains(name);
  }

  /**
   * Returns the Elasticsearch index privileges a Gravitino privilege maps to.
   *
   * @param name the Gravitino privilege name.
   * @return the index privileges, empty when the privilege has no Elasticsearch counterpart.
   */
  public static Set<String> indexPrivileges(Privilege.Name name) {
    return MAPPING.getOrDefault(name, Collections.emptySet());
  }

  /**
   * Returns whether a Gravitino privilege has an Elasticsearch counterpart.
   *
   * @param name the Gravitino privilege name.
   * @return true when the privilege translates to at least one index privilege.
   */
  public static boolean isSupported(Privilege.Name name) {
    return MAPPING.containsKey(name);
  }

  /**
   * Returns whether an Elasticsearch index privilege reads document contents. Row and column
   * restrictions only apply to reads, so a role carrying them must not also carry write access.
   *
   * @param privilege the Elasticsearch index privilege.
   * @return true when the privilege reads documents.
   */
  public static boolean isReadPrivilege(String privilege) {
    return READ.equals(privilege);
  }
}
