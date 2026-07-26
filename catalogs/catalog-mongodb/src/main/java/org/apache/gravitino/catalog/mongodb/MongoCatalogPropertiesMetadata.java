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
import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.catalog.mongodb.converter.MongoSchemaConfig;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/** Catalog level properties for the MongoDB catalog. */
public class MongoCatalogPropertiesMetadata extends BasePropertiesMetadata {

  /** Standard MongoDB connection string, normally carrying credentials. */
  public static final String CONNECTION_URI = "mongodb.connection-uri";

  private static final Map<String, PropertyEntry<?>> PROPERTIES_METADATA =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              CONNECTION_URI,
              PropertyEntry.stringRequiredPropertyEntry(
                  CONNECTION_URI,
                  "MongoDB connection string. Hidden because it normally carries credentials.",
                  false,
                  true))
          .put(
              MongoSchemaConfig.INFERENCE_MODE,
              PropertyEntry.stringOptionalPropertyEntry(
                  MongoSchemaConfig.INFERENCE_MODE,
                  "Where column definitions come from: validator, sample, or validator-then-sample.",
                  false,
                  "validator-then-sample",
                  false))
          .put(
              MongoSchemaConfig.SAMPLE_SIZE,
              PropertyEntry.integerOptionalPropertyEntry(
                  MongoSchemaConfig.SAMPLE_SIZE,
                  "Number of documents drawn when inferring a schema by sampling.",
                  false,
                  1000,
                  false))
          .put(
              MongoSchemaConfig.CONFLICT_POLICY,
              PropertyEntry.stringOptionalPropertyEntry(
                  MongoSchemaConfig.CONFLICT_POLICY,
                  "Behavior when one field is observed with incompatible types: widen-to-string, "
                      + "external-type, or fail.",
                  false,
                  "widen-to-string",
                  false))
          .put(
              MongoSchemaConfig.DECIMAL_SCALE,
              PropertyEntry.integerOptionalPropertyEntry(
                  MongoSchemaConfig.DECIMAL_SCALE,
                  "Scale applied to BSON Decimal128, which carries no declared scale of its own.",
                  false,
                  8,
                  false))
          .put(
              MongoSchemaConfig.MAX_NESTING_DEPTH,
              PropertyEntry.integerOptionalPropertyEntry(
                  MongoSchemaConfig.MAX_NESTING_DEPTH,
                  "Maximum nesting depth mapped to struct types before falling back to string.",
                  false,
                  8,
                  false))
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return PROPERTIES_METADATA;
  }

  /**
   * Builds a schema derivation config from raw catalog properties, leaving anything absent at its
   * default.
   *
   * @param conf the catalog properties
   * @return the derived config
   */
  public static MongoSchemaConfig toSchemaConfig(Map<String, String> conf) {
    MongoSchemaConfig.Builder builder = MongoSchemaConfig.builder();

    String mode = conf.get(MongoSchemaConfig.INFERENCE_MODE);
    if (mode != null) {
      builder.inferenceMode(MongoSchemaConfig.InferenceMode.valueOf(normalizeEnum(mode)));
    }

    String policy = conf.get(MongoSchemaConfig.CONFLICT_POLICY);
    if (policy != null) {
      builder.conflictPolicy(MongoSchemaConfig.ConflictPolicy.valueOf(normalizeEnum(policy)));
    }

    String sampleSize = conf.get(MongoSchemaConfig.SAMPLE_SIZE);
    if (sampleSize != null) {
      builder.sampleSize(Integer.parseInt(sampleSize.trim()));
    }

    String decimalScale = conf.get(MongoSchemaConfig.DECIMAL_SCALE);
    if (decimalScale != null) {
      builder.decimalScale(Integer.parseInt(decimalScale.trim()));
    }

    String nestingDepth = conf.get(MongoSchemaConfig.MAX_NESTING_DEPTH);
    if (nestingDepth != null) {
      builder.maxNestingDepth(Integer.parseInt(nestingDepth.trim()));
    }

    return builder.build();
  }

  private static String normalizeEnum(String value) {
    return value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
  }
}
