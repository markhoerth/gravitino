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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.apache.gravitino.rel.Column;
import org.junit.jupiter.api.Test;

/**
 * Covers how a column comment is chosen when the mapping's {@code meta} block is malformed.
 *
 * <p>A live cluster rejects most of these, since Elasticsearch constrains {@code meta} to an object
 * of string values. A mapping can reach this catalog from a snapshot, a proxy or a hand written
 * template, so each shape has to land on the synthesized fallback rather than throw.
 *
 * <p>The well formed cases run against a real cluster in {@code CatalogElasticsearchIT}.
 */
public class ElasticsearchMappingParserCommentTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** What a keyword field with no usable description reports. */
  private static final String SYNTHESIZED = "elasticsearch type: keyword";

  /** A field with no meta block at all keeps the synthesized comment. */
  @Test
  public void testNoMeta() {
    assertEquals(SYNTHESIZED, commentOf("{\"type\":\"keyword\"}"));
  }

  /** An empty meta block carries no description, so the synthesized comment stands. */
  @Test
  public void testEmptyMeta() {
    assertEquals(SYNTHESIZED, commentOf("{\"type\":\"keyword\",\"meta\":{}}"));
  }

  /** A blank description does not shadow the fallback. */
  @Test
  public void testBlankDescription() {
    assertEquals(
        SYNTHESIZED, commentOf("{\"type\":\"keyword\",\"meta\":{\"description\":\"   \"}}"));
  }

  /** With both keys present, description wins over comment. */
  @Test
  public void testDescriptionWinsOverComment() {
    assertEquals(
        "the description",
        commentOf(
            "{\"type\":\"keyword\",\"meta\":"
                + "{\"description\":\"the description\",\"comment\":\"the comment\"}}"));
  }

  /** A blank description falls through to comment rather than to the synthesized string. */
  @Test
  public void testBlankDescriptionFallsThroughToComment() {
    assertEquals(
        "the comment",
        commentOf(
            "{\"type\":\"keyword\",\"meta\":{\"description\":\"\",\"comment\":\"the comment\"}}"));
  }

  /** A meta block that is a string rather than an object falls back instead of throwing. */
  @Test
  public void testMetaIsNotAnObject() {
    String mapping = "{\"type\":\"keyword\",\"meta\":\"primary id\"}";
    assertDoesNotThrow(() -> commentOf(mapping));
    assertEquals(SYNTHESIZED, commentOf(mapping));
  }

  /** The comment of the single field in a mapping declaring {@code field} with the given node. */
  private static String commentOf(String fieldJson) {
    JsonNode mappings = parse("{\"properties\":{\"field\":" + fieldJson + "}}");

    List<Column> columns = ElasticsearchMappingParser.parseColumns(mappings);
    assertEquals(1, columns.size());
    return columns.get(0).comment();
  }

  private static ObjectNode parse(String json) {
    try {
      return (ObjectNode) MAPPER.readTree(json);
    } catch (Exception e) {
      throw new IllegalStateException("Malformed test mapping: " + json, e);
    }
  }
}
