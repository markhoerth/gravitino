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

import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/**
 * Maps a single Elasticsearch mapping type name to a Gravitino type.
 *
 * <p>Anything without a faithful Gravitino equivalent becomes an {@link Types.ExternalType} rather
 * than being coerced into a lookalike. Trino and Spark treat an external type as non-projectable,
 * which is the correct signal for a field they cannot read through Gravitino anyway.
 *
 * <p>{@code dense_vector} is the deliberate exception: it is a fixed length float array, and vector
 * retrieval is the reason this connector exists, so it maps to {@code list<float>} with its
 * dimension carried in the column comment.
 */
public final class ElasticsearchTypeConverter {

  /** Mapping type for an object field, whose shape comes from its own {@code properties}. */
  public static final String OBJECT = "object";

  /** Mapping type for an array of independently indexed objects. */
  public static final String NESTED = "nested";

  /** Mapping type for a field that is an alternative name for another field. */
  public static final String ALIAS = "alias";

  /** Mapping type for a fixed length float vector. */
  public static final String DENSE_VECTOR = "dense_vector";

  private ElasticsearchTypeConverter() {}

  /**
   * Converts one Elasticsearch mapping type to a Gravitino type.
   *
   * <p>Structural types are rejected rather than guessed at: {@code object} and {@code nested}
   * cannot be resolved from the type name alone, and {@code alias} needs the whole mapping to find
   * its target. {@link ElasticsearchMappingParser} handles all three before reaching here.
   *
   * @param esType the {@code type} declared on a mapping node
   * @return the corresponding Gravitino type
   * @throws IllegalArgumentException if the type is structural or absent
   */
  public static Type fromMappingType(String esType) {
    if (esType == null) {
      throw new IllegalArgumentException("Elasticsearch mapping type is required");
    }

    switch (esType) {
      case "keyword":
      case "constant_keyword":
      case "wildcard":
      case "text":
      case "match_only_text":
      case "annotated_text":
      case "version":
      case "ip":
        return Types.StringType.get();

      case "long":
      case "unsigned_long":
        return Types.LongType.get();

      case "integer":
        return Types.IntegerType.get();

      case "short":
        return Types.ShortType.get();

      case "byte":
        return Types.ByteType.get();

      case "double":
      case "scaled_float":
        return Types.DoubleType.get();

      case "float":
      case "half_float":
        return Types.FloatType.get();

      case "boolean":
        return Types.BooleanType.get();

      case "date":
      case "date_nanos":
        return Types.TimestampType.withTimeZone();

      case "binary":
        return Types.BinaryType.get();

      case DENSE_VECTOR:
        return Types.ListType.of(Types.FloatType.get(), false);

      case OBJECT:
      case NESTED:
      case ALIAS:
        throw new IllegalArgumentException(
            "Elasticsearch mapping type " + esType + " must be resolved against the full mapping");

      default:
        return Types.ExternalType.of(esType);
    }
  }

  /**
   * Reports whether a mapping type carries its shape in its own {@code properties} rather than in
   * its type name.
   *
   * @param esType the {@code type} declared on a mapping node
   * @return true for {@code object} and {@code nested}
   */
  public static boolean isStructural(String esType) {
    return OBJECT.equals(esType) || NESTED.equals(esType);
  }
}
