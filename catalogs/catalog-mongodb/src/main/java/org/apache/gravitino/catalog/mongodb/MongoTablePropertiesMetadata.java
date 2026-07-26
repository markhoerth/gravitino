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
package org.apache.gravitino.catalog.mongodb;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/**
 * Table level properties. These are reported from the collection's own options rather than set by
 * callers, since this catalog does not create collections.
 */
public class MongoTablePropertiesMetadata extends BasePropertiesMetadata {

  /** Whether the collection declares a {@code $jsonSchema} validator. */
  public static final String HAS_VALIDATOR = "mongodb.has-validator";

  /** Whether the collection is capped. */
  public static final String CAPPED = "mongodb.capped";

  /** The collection's validation level, when it has a validator. */
  public static final String VALIDATION_LEVEL = "mongodb.validation-level";

  /** The collection's validation action, when it has a validator. */
  public static final String VALIDATION_ACTION = "mongodb.validation-action";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              HAS_VALIDATOR,
              PropertyEntry.stringOptionalPropertyEntry(
                  HAS_VALIDATOR,
                  "Whether the collection declares a $jsonSchema validator.",
                  true,
                  "false",
                  false))
          .put(
              CAPPED,
              PropertyEntry.stringOptionalPropertyEntry(
                  CAPPED, "Whether the collection is capped.", true, "false", false))
          .put(
              VALIDATION_LEVEL,
              PropertyEntry.stringOptionalPropertyEntry(
                  VALIDATION_LEVEL, "The collection's validation level.", true, null, false))
          .put(
              VALIDATION_ACTION,
              PropertyEntry.stringOptionalPropertyEntry(
                  VALIDATION_ACTION, "The collection's validation action.", true, null, false))
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }
}
