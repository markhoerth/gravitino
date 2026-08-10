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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.List;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Covers the whole mapping to columns path: ordering, nullability, the structural types, and the
 * comment format that carries everything Gravitino's type system cannot.
 */
public class ElasticsearchMappingParserTest {

  @Test
  public void testColumnsKeepMappingOrderAndAreAllNullable() {
    List<Column> columns = ElasticsearchMappingParser.parseColumns(fixture("flat-scalars.json"));

    assertEquals(21, columns.size());
    assertEquals("sku", columns.get(0).name());
    assertEquals("bytes_total", columns.get(8).name());
    assertEquals("thumbnail", columns.get(20).name());

    for (Column column : columns) {
      assertTrue(
          column.nullable(),
          column.name() + " must be nullable: a mapping declares shape, never presence");
    }
  }

  @Test
  public void testScalarColumnCarriesItsElasticsearchTypeInTheComment() {
    Column sku = column("flat-scalars.json", "sku");

    assertEquals(Types.StringType.get(), sku.dataType());
    assertEquals("elasticsearch type: keyword", sku.comment());
  }

  @Test
  public void testObjectRecursesThreeLevelsDepthFirst() {
    Column customer = column("nested-object.json", "customer");
    assertEquals("elasticsearch type: object", customer.comment());

    Types.StructType level1 = assertInstanceOf(Types.StructType.class, customer.dataType());
    assertEquals(2, level1.fields().length);
    assertEquals("id", level1.fields()[0].name());
    assertEquals(Types.StringType.get(), level1.fields()[0].type());

    Types.StructType level2 =
        assertInstanceOf(Types.StructType.class, level1.fields()[1].type(), "customer.address");
    assertEquals("address", level1.fields()[1].name());
    assertEquals("city", level2.fields()[0].name());

    Types.StructType level3 =
        assertInstanceOf(Types.StructType.class, level2.fields()[1].type(), "customer.address.geo");
    assertEquals("geo", level2.fields()[1].name());
    assertEquals(2, level3.fields().length);
    assertEquals(Types.DoubleType.get(), level3.fields()[0].type());
    assertTrue(level3.fields()[0].nullable(), "Nested fields are nullable too");
  }

  @Test
  public void testObjectWithoutPropertiesHasNoProjectableShape() {
    Column opaque = column("nested-object.json", "opaque");
    assertEquals(Types.ExternalType.of("object"), opaque.dataType());
  }

  @Test
  public void testNestedBecomesAListOfStructs() {
    Column lineItems = column("nested-array.json", "line_items");
    assertEquals("elasticsearch type: nested", lineItems.comment());

    Types.ListType list = assertInstanceOf(Types.ListType.class, lineItems.dataType());
    assertTrue(list.elementNullable());

    Types.StructType element = assertInstanceOf(Types.StructType.class, list.elementType());
    assertEquals(2, element.fields().length);
    assertEquals("sku", element.fields()[0].name());
    assertEquals(Types.IntegerType.get(), element.fields()[1].type());
  }

  @Test
  public void testDenseVectorCarriesDimsAndSimilarityInTheComment() {
    Column embedding = column("dense-vector.json", "embedding");

    assertEquals(Types.ListType.of(Types.FloatType.get(), false), embedding.dataType());
    assertEquals(
        "elasticsearch type: dense_vector; dims: 1024; similarity: cosine; index: true",
        embedding.comment());
  }

  @Test
  public void testDenseVectorWithoutOptionalSettingsOmitsThem() {
    Column bare = column("dense-vector.json", "bare_embedding");

    assertEquals(Types.ListType.of(Types.FloatType.get(), false), bare.dataType());
    assertEquals("elasticsearch type: dense_vector", bare.comment());
  }

  @Test
  public void testMultiFieldsAreCommentedRatherThanEmittedAsColumns() {
    List<Column> columns = ElasticsearchMappingParser.parseColumns(fixture("multi-fields.json"));

    assertEquals(1, columns.size(), "title.raw must not become a sibling column");
    assertEquals("title", columns.get(0).name());
    assertEquals(Types.StringType.get(), columns.get(0).dataType());
    assertEquals(
        "elasticsearch type: text; multi-fields: raw (keyword), english (text)",
        columns.get(0).comment());
  }

  @Test
  public void testAliasResolvesToItsTargetTypeAndRecordsTheSource() {
    Column alias = column("alias.json", "customer_id");

    assertEquals(Types.StringType.get(), alias.dataType());
    assertEquals("elasticsearch type: keyword; alias for: customer.id", alias.comment());
  }

  @Test
  public void testAliasPointingNowhereStaysExternalRatherThanGuessing() {
    Column dangling = column("alias.json", "dangling_id");

    assertEquals(Types.ExternalType.of("alias"), dangling.dataType());
    assertEquals("elasticsearch type: alias; alias for: nowhere.at.all", dangling.comment());
  }

  @Test
  public void testFutureTypesLandAsExternalTypesInsteadOfThrowing() {
    List<Column> columns = ElasticsearchMappingParser.parseColumns(fixture("future-types.json"));

    assertEquals(5, columns.size());
    for (Column column : columns) {
      Type type = column.dataType();
      assertInstanceOf(
          Types.ExternalType.class, type, column.name() + " must not be coerced into a lookalike");
    }
    assertEquals(
        Types.ExternalType.of("quantum_entangled_keyword"),
        columns.get(4).dataType(),
        "A type this connector has never heard of must still parse");
  }

  @Test
  public void testMappingWithoutPropertiesYieldsNoColumns() {
    assertTrue(
        ElasticsearchMappingParser.parseColumns(JsonNodeFactory.instance.objectNode()).isEmpty());
    assertTrue(ElasticsearchMappingParser.parseColumns(null).isEmpty());
  }

  private static JsonNode fixture(String name) {
    return MappingFixtures.load(name);
  }

  private static Column column(String fixtureName, String name) {
    for (Column column : ElasticsearchMappingParser.parseColumns(fixture(fixtureName))) {
      if (column.name().equals(name)) {
        return column;
      }
    }
    return fail("Fixture " + fixtureName + " has no column named " + name);
  }
}
