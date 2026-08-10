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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/**
 * Table level properties, all of them reported by Elasticsearch rather than set by callers. They
 * are reserved because this catalog never creates or alters an index, so a value supplied by a
 * caller could only ever be wrong.
 */
public class ElasticsearchTablePropertiesMetadata extends BasePropertiesMetadata {

  /** The index's UUID, from the index settings. */
  public static final String INDEX_UUID = "index.uuid";

  /** The index's primary shard count, from the index settings. */
  public static final String NUMBER_OF_SHARDS = "index.number_of_shards";

  /** The index's replica count, from the index settings. */
  public static final String NUMBER_OF_REPLICAS = "index.number_of_replicas";

  /** The index's creation date in epoch milliseconds, from the index settings. */
  public static final String CREATION_DATE = "index.creation_date";

  /** Document count, present only when the index was seen through a listing. */
  public static final String DOCS_COUNT = "docs.count";

  /** Store size, present only when the index was seen through a listing. */
  public static final String STORE_SIZE = "store.size";

  /** Open or close, present only when the index was seen through a listing. */
  public static final String STATUS = "status";

  /** Green, yellow or red, present only when the index was seen through a listing. */
  public static final String HEALTH = "health";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              INDEX_UUID,
              PropertyEntry.stringReservedPropertyEntry(INDEX_UUID, "The index's UUID.", false))
          .put(
              NUMBER_OF_SHARDS,
              PropertyEntry.stringReservedPropertyEntry(
                  NUMBER_OF_SHARDS, "The index's primary shard count.", false))
          .put(
              NUMBER_OF_REPLICAS,
              PropertyEntry.stringReservedPropertyEntry(
                  NUMBER_OF_REPLICAS, "The index's replica count.", false))
          .put(
              CREATION_DATE,
              PropertyEntry.stringReservedPropertyEntry(
                  CREATION_DATE, "The index's creation date in epoch milliseconds.", false))
          .put(
              DOCS_COUNT,
              PropertyEntry.stringReservedPropertyEntry(
                  DOCS_COUNT, "Document count, as reported by the index listing.", false))
          .put(
              STORE_SIZE,
              PropertyEntry.stringReservedPropertyEntry(
                  STORE_SIZE, "Store size, as reported by the index listing.", false))
          .put(
              STATUS,
              PropertyEntry.stringReservedPropertyEntry(
                  STATUS, "Whether the index is open or closed.", false))
          .put(
              HEALTH,
              PropertyEntry.stringReservedPropertyEntry(
                  HEALTH, "The index's health: green, yellow or red.", false))
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
