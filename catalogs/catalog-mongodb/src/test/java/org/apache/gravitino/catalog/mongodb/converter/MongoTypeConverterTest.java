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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

/**
 * Covers the BSON to Gravitino mapping and, more importantly, the widening lattice in {@link
 * MongoTypeConverter#unify}. The lattice is where this connector's opinion about inconsistent
 * documents actually lives, so each rule gets an explicit case.
 */
public class MongoTypeConverterTest {

  private static final int TOP_LEVEL = 1;

  private final MongoSchemaConfig defaultConfig = MongoSchemaConfig.defaults();
  private final MongoTypeConverter converter = new MongoTypeConverter(defaultConfig);

  private MongoTypeConverter converterWith(MongoSchemaConfig.ConflictPolicy policy) {
    return new MongoTypeConverter(MongoSchemaConfig.builder().conflictPolicy(policy).build());
  }

  @Test
  public void testScalarValuesMapToExpectedTypes() {
    assertEquals(Types.BooleanType.get(), converter.fromValue(BsonBoolean.TRUE, TOP_LEVEL));
    assertEquals(Types.IntegerType.get(), converter.fromValue(new BsonInt32(1), TOP_LEVEL));
    assertEquals(Types.LongType.get(), converter.fromValue(new BsonInt64(1L), TOP_LEVEL));
    assertEquals(Types.DoubleType.get(), converter.fromValue(new BsonDouble(1.0), TOP_LEVEL));
    assertEquals(Types.StringType.get(), converter.fromValue(new BsonString("a"), TOP_LEVEL));
    assertEquals(
        Types.TimestampType.withTimeZone(), converter.fromValue(new BsonDateTime(0L), TOP_LEVEL));
    assertEquals(Types.NullType.get(), converter.fromValue(BsonNull.VALUE, TOP_LEVEL));
  }

  @Test
  public void testObjectIdMapsToExternalType() {
    Type type = converter.fromValue(new BsonObjectId(new ObjectId()), TOP_LEVEL);
    assertEquals(Types.ExternalType.of(MongoTypeConverter.OBJECT_ID_TYPE), type);
  }

  @Test
  public void testDecimal128UsesConfiguredScale() {
    MongoTypeConverter scaled =
        new MongoTypeConverter(MongoSchemaConfig.builder().decimalScale(4).build());
    Type type =
        scaled.fromValue(new BsonDecimal128(new Decimal128(new BigDecimal("1.2345"))), TOP_LEVEL);
    assertEquals(Types.DecimalType.of(MongoSchemaConfig.DECIMAL128_PRECISION, 4), type);
  }

  @Test
  public void testNestedDocumentBecomesStruct() {
    BsonDocument document = new BsonDocument("inner", new BsonDocument("count", new BsonInt32(3)));
    Type type = converter.fromValue(document, TOP_LEVEL);

    Types.StructType expected =
        Types.StructType.of(
            Types.StructType.Field.nullableField(
                "inner",
                Types.StructType.of(
                    Types.StructType.Field.nullableField("count", Types.IntegerType.get()))));
    assertEquals(expected, type);
  }

  @Test
  public void testArrayElementsAreUnified() {
    BsonArray array = new BsonArray(Arrays.asList(new BsonInt32(1), new BsonInt64(2L)));
    assertEquals(
        Types.ListType.of(Types.LongType.get(), true), converter.fromValue(array, TOP_LEVEL));
  }

  @Test
  public void testNestingBeyondMaxDepthFallsBackToString() {
    MongoTypeConverter shallow =
        new MongoTypeConverter(MongoSchemaConfig.builder().maxNestingDepth(2).build());
    BsonDocument document =
        new BsonDocument("a", new BsonDocument("b", new BsonDocument("c", new BsonInt32(1))));

    Types.StructType expected =
        Types.StructType.of(Types.StructType.Field.nullableField("a", Types.StringType.get()));
    assertEquals(expected, shallow.fromValue(document, TOP_LEVEL));
  }

  @Test
  public void testIntegerWidensToLong() {
    assertEquals(
        Types.LongType.get(), converter.unify(Types.IntegerType.get(), Types.LongType.get()));
    assertEquals(
        Types.LongType.get(), converter.unify(Types.LongType.get(), Types.IntegerType.get()));
  }

  @Test
  public void testIntegerWidensToDouble() {
    assertEquals(
        Types.DoubleType.get(), converter.unify(Types.IntegerType.get(), Types.DoubleType.get()));
  }

  @Test
  public void testDoubleWidensToDecimal() {
    Type decimal = Types.DecimalType.of(MongoSchemaConfig.DECIMAL128_PRECISION, 8);
    assertEquals(decimal, converter.unify(Types.DoubleType.get(), decimal));
  }

  @Test
  public void testNullIsAbsorbedFromEitherSide() {
    assertEquals(
        Types.StringType.get(), converter.unify(Types.NullType.get(), Types.StringType.get()));
    assertEquals(
        Types.StringType.get(), converter.unify(Types.StringType.get(), Types.NullType.get()));
  }

  @Test
  public void testStructMergeMakesUnmatchedFieldsNullable() {
    Types.StructType left =
        Types.StructType.of(
            Types.StructType.Field.notNullField("shared", Types.IntegerType.get()),
            Types.StructType.Field.notNullField("onlyLeft", Types.StringType.get()));
    Types.StructType right =
        Types.StructType.of(
            Types.StructType.Field.notNullField("shared", Types.LongType.get()),
            Types.StructType.Field.notNullField("onlyRight", Types.BooleanType.get()));

    Types.StructType merged = (Types.StructType) converter.unify(left, right);

    assertEquals(3, merged.fields().length);
    for (Types.StructType.Field field : merged.fields()) {
      assertTrue(field.nullable(), field.name() + " should be nullable after a merge");
      if ("shared".equals(field.name())) {
        assertEquals(Types.LongType.get(), field.type());
      }
    }
  }

  @Test
  public void testListElementsAreUnifiedOnMerge() {
    Type left = Types.ListType.of(Types.IntegerType.get(), true);
    Type right = Types.ListType.of(Types.DoubleType.get(), true);
    assertEquals(Types.ListType.of(Types.DoubleType.get(), true), converter.unify(left, right));
  }

  @Test
  public void testConflictWidensToStringByDefault() {
    assertEquals(
        Types.StringType.get(), converter.unify(Types.StringType.get(), Types.BooleanType.get()));
  }

  @Test
  public void testConflictCanProduceExternalType() {
    MongoTypeConverter external = converterWith(MongoSchemaConfig.ConflictPolicy.EXTERNAL_TYPE);
    Type type = external.unify(Types.StringType.get(), Types.BooleanType.get());
    assertInstanceOf(Types.ExternalType.class, type);
  }

  @Test
  public void testConflictCanFail() {
    MongoTypeConverter strict = converterWith(MongoSchemaConfig.ConflictPolicy.FAIL);
    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> strict.unify(Types.StringType.get(), Types.BooleanType.get()));
    assertTrue(error.getMessage().contains(MongoSchemaConfig.CONFLICT_POLICY));
  }

  @Test
  public void testSchemaNodeWithNullableUnionResolvesToConcreteType() {
    Document node = new Document("bsonType", ImmutableList.of("string", "null"));
    assertEquals(Types.StringType.get(), converter.fromSchemaNode(node, TOP_LEVEL));
  }

  @Test
  public void testSchemaNodeRequiredFieldIsNotNullable() {
    Document node =
        new Document("bsonType", "object")
            .append("properties", new Document("id", new Document("bsonType", "int")))
            .append("required", ImmutableList.of("id"));

    Types.StructType struct = (Types.StructType) converter.fromSchemaNode(node, TOP_LEVEL);
    assertEquals(1, struct.fields().length);
    assertEquals("id", struct.fields()[0].name());
    assertTrue(!struct.fields()[0].nullable());
  }

  @Test
  public void testUnknownBsonTypeBecomesExternalType() {
    Document node = new Document("bsonType", "minKey");
    assertInstanceOf(Types.ExternalType.class, converter.fromSchemaNode(node, TOP_LEVEL));
  }
}
