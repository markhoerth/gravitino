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
package org.apache.gravitino.catalog.mongodb.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Covers validator generation. The round trip case is the one that matters: columns handed to
 * {@code createTable} must come back unchanged from a later {@code loadTable}, which is what
 * separates a governed catalog from a sampling heuristic.
 */
public class MongoValidatorBuilderTest {

  private static final int TOP_LEVEL = 1;

  private final MongoSchemaConfig config = MongoSchemaConfig.defaults();
  private final MongoValidatorBuilder builder = new MongoValidatorBuilder(config);
  private final MongoTypeConverter converter = new MongoTypeConverter(config);

  private Column column(String name, Type type, boolean nullable) {
    return Column.of(name, type, null, nullable, false, Column.DEFAULT_VALUE_NOT_SET);
  }

  @Test
  public void testValidatorIsWrappedInJsonSchema() {
    Document validator =
        builder.buildValidator(new Column[] {column("id", Types.IntegerType.get(), false)});
    assertNotNull(validator.get("$jsonSchema", Document.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testOnlyNonNullableColumnsAreRequired() {
    Column[] columns = {
      column("id", Types.IntegerType.get(), false), column("name", Types.StringType.get(), true)
    };

    Document schema = builder.buildJsonSchema(columns);
    List<String> required = (List<String>) schema.get("required");

    assertEquals(1, required.size());
    assertTrue(required.contains("id"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNullableColumnAcceptsNullInBsonType() {
    Document schema =
        builder.buildJsonSchema(new Column[] {column("name", Types.StringType.get(), true)});
    Document properties = schema.get("properties", Document.class);
    List<String> bsonTypes = (List<String>) properties.get("name", Document.class).get("bsonType");

    assertTrue(bsonTypes.contains("string"));
    assertTrue(bsonTypes.contains("null"));
  }

  @Test
  public void testNonNullableColumnUsesScalarBsonType() {
    Document schema =
        builder.buildJsonSchema(new Column[] {column("id", Types.IntegerType.get(), false)});
    Document properties = schema.get("properties", Document.class);

    assertEquals("int", properties.get("id", Document.class).get("bsonType"));
  }

  @Test
  public void testAdditionalPropertiesFollowsConfig() {
    MongoValidatorBuilder strict =
        new MongoValidatorBuilder(
            MongoSchemaConfig.builder().allowAdditionalProperties(false).build());
    Document schema =
        strict.buildJsonSchema(new Column[] {column("id", Types.IntegerType.get(), false)});

    assertFalse((Boolean) schema.get("additionalProperties"));
  }

  @Test
  public void testCollModCarriesValidatorAndLevels() {
    Document command =
        builder.buildCollMod(
            "orders",
            new Column[] {column("id", Types.IntegerType.get(), false)},
            "moderate",
            "error");

    assertEquals("orders", command.get("collMod"));
    assertEquals("moderate", command.get("validationLevel"));
    assertEquals("error", command.get("validationAction"));
    assertNotNull(command.get("validator", Document.class));
  }

  @Test
  public void testColumnTypesSurviveTheRoundTrip() {
    Column[] columns = {
      column("id", Types.ExternalType.of(MongoTypeConverter.OBJECT_ID_TYPE), false),
      column("name", Types.StringType.get(), true),
      column("quantity", Types.IntegerType.get(), true),
      column("total", Types.DecimalType.of(MongoSchemaConfig.DECIMAL128_PRECISION, 8), true),
      column("placed_at", Types.TimestampType.withTimeZone(), true),
      column("tags", Types.ListType.of(Types.StringType.get(), true), true),
      column(
          "address",
          Types.StructType.of(
              Types.StructType.Field.notNullField("city", Types.StringType.get()),
              Types.StructType.Field.nullableField("zip", Types.StringType.get())),
          true)
    };

    Document schema = builder.buildJsonSchema(columns);
    Document properties = schema.get("properties", Document.class);

    for (Column original : columns) {
      Document node = properties.get(original.name(), Document.class);
      Type resolved = converter.fromSchemaNode(node, TOP_LEVEL);
      assertEquals(
          original.dataType(),
          resolved,
          "column " + original.name() + " did not survive a round trip");
    }
  }
}
