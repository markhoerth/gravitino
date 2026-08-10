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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Covers the single mapping type to Gravitino type rule. The scalar sweep is driven by the same
 * flat fixture the parser test uses, so a type added to the fixture and not to the converter fails
 * here rather than silently becoming an external type in production.
 */
public class ElasticsearchTypeConverterTest {

  /** Expected Gravitino type for every scalar in {@code mappings/flat-scalars.json}. */
  private static final Map<String, Type> EXPECTED_SCALARS =
      ImmutableMap.<String, Type>builder()
          .put("keyword", Types.StringType.get())
          .put("constant_keyword", Types.StringType.get())
          .put("wildcard", Types.StringType.get())
          .put("text", Types.StringType.get())
          .put("match_only_text", Types.StringType.get())
          .put("annotated_text", Types.StringType.get())
          .put("version", Types.StringType.get())
          .put("ip", Types.StringType.get())
          .put("long", Types.LongType.get())
          .put("unsigned_long", Types.LongType.get())
          .put("integer", Types.IntegerType.get())
          .put("short", Types.ShortType.get())
          .put("byte", Types.ByteType.get())
          .put("double", Types.DoubleType.get())
          .put("scaled_float", Types.DoubleType.get())
          .put("float", Types.FloatType.get())
          .put("half_float", Types.FloatType.get())
          .put("boolean", Types.BooleanType.get())
          .put("date", Types.TimestampType.withTimeZone())
          .put("date_nanos", Types.TimestampType.withTimeZone())
          .put("binary", Types.BinaryType.get())
          .build();

  @Test
  public void testEveryScalarInTheFixtureMapsAsDocumented() {
    JsonNode properties = MappingFixtures.load("flat-scalars.json").get("properties");

    int checked = 0;
    for (JsonNode field : properties) {
      String esType = field.get("type").asText();
      Type expected = EXPECTED_SCALARS.get(esType);
      assertNotNull(expected, "Fixture carries an unmapped type: " + esType);
      assertEquals(
          expected,
          ElasticsearchTypeConverter.fromMappingType(esType),
          "Wrong Gravitino type for " + esType);
      checked++;
    }

    assertEquals(
        EXPECTED_SCALARS.size(), checked, "The fixture must exercise every documented scalar");
  }

  @Test
  public void testDenseVectorMapsToNonNullableFloatList() {
    Type type = ElasticsearchTypeConverter.fromMappingType(ElasticsearchTypeConverter.DENSE_VECTOR);

    assertEquals(Types.ListType.of(Types.FloatType.get(), false), type);
    assertFalse(
        ((Types.ListType) type).elementNullable(),
        "A vector slot is always populated, so its elements are not nullable");
  }

  @Test
  public void testUnmappableTypesBecomeExternalTypes() {
    JsonNode properties = MappingFixtures.load("future-types.json").get("properties");

    for (JsonNode field : properties) {
      String esType = field.get("type").asText();
      assertEquals(
          Types.ExternalType.of(esType),
          ElasticsearchTypeConverter.fromMappingType(esType),
          esType + " has no faithful Gravitino equivalent and must stay external");
    }
  }

  @Test
  public void testUnrecognizedFutureTypeIsCarriedRatherThanThrown() {
    assertEquals(
        Types.ExternalType.of("quantum_entangled_keyword"),
        ElasticsearchTypeConverter.fromMappingType("quantum_entangled_keyword"));
  }

  @Test
  public void testStructuralTypesAreRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ElasticsearchTypeConverter.fromMappingType(ElasticsearchTypeConverter.OBJECT));
    assertThrows(
        IllegalArgumentException.class,
        () -> ElasticsearchTypeConverter.fromMappingType(ElasticsearchTypeConverter.NESTED));
    assertThrows(
        IllegalArgumentException.class,
        () -> ElasticsearchTypeConverter.fromMappingType(ElasticsearchTypeConverter.ALIAS));
    assertThrows(
        IllegalArgumentException.class, () -> ElasticsearchTypeConverter.fromMappingType(null));
  }

  @Test
  public void testOnlyObjectAndNestedAreStructural() {
    assertTrue(ElasticsearchTypeConverter.isStructural(ElasticsearchTypeConverter.OBJECT));
    assertTrue(ElasticsearchTypeConverter.isStructural(ElasticsearchTypeConverter.NESTED));
    assertFalse(ElasticsearchTypeConverter.isStructural(ElasticsearchTypeConverter.ALIAS));
    assertFalse(ElasticsearchTypeConverter.isStructural("keyword"));
  }
}
