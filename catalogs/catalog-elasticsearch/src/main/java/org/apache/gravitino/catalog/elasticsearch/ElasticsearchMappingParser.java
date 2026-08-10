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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/**
 * Turns an index mapping into an ordered list of Gravitino columns.
 *
 * <p>Every column is nullable. A mapping declares the shape a field takes when it is present and
 * says nothing about whether any document actually carries it, so {@code NOT NULL} would never be
 * justified. Columns keep the mapping's own order, with nested {@code properties} recursed depth
 * first.
 *
 * <p>Multi-fields do not become sibling columns. Emitting {@code title} alongside {@code title.raw}
 * doubles the column count for no query benefit through Gravitino, so the multi-fields are recorded
 * in the parent column's comment instead.
 *
 * <p>Runtime fields are a known gap. They live outside {@code properties} and are computed per
 * query, so they are not surfaced at all.
 */
public final class ElasticsearchMappingParser {

  private static final String PROPERTIES = "properties";
  private static final String TYPE = "type";
  private static final String FIELDS = "fields";
  private static final String PATH = "path";
  private static final String DIMS = "dims";
  private static final String SIMILARITY = "similarity";
  private static final String INDEX = "index";

  /** Mapping type reported for a node that declares neither a type nor any properties. */
  private static final String UNKNOWN_TYPE = "unknown";

  /** Bound on alias chains, so a mapping whose aliases point at each other cannot spin. */
  private static final int MAX_ALIAS_HOPS = 10;

  private ElasticsearchMappingParser() {}

  /**
   * Parses the {@code mappings} node of an index into columns.
   *
   * @param mappings the node holding the index's {@code properties}, as returned under {@code
   *     <index>.mappings} by the mapping API
   * @return the columns in mapping order, empty if the index declares no properties
   */
  public static List<Column> parseColumns(JsonNode mappings) {
    JsonNode properties = properties(mappings);
    if (properties == null) {
      return ImmutableList.of();
    }

    ImmutableList.Builder<Column> columns = ImmutableList.builder();
    Iterator<Map.Entry<String, JsonNode>> fields = properties.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      columns.add(toColumn(mappings, field.getKey(), field.getValue()));
    }
    return columns.build();
  }

  private static Column toColumn(JsonNode root, String name, JsonNode node) {
    Resolved resolved = resolve(root, node);
    return Column.of(
        name, resolved.type, resolved.comment, true, false, Column.DEFAULT_VALUE_NOT_SET);
  }

  /**
   * Resolves one mapping node to a type and a comment, following an alias to its target first so
   * that the alias reports the type it actually stands for.
   */
  private static Resolved resolve(JsonNode root, JsonNode node) {
    String aliasPath = null;
    JsonNode effective = node;

    if (ElasticsearchTypeConverter.ALIAS.equals(declaredType(node))) {
      aliasPath = text(node, PATH);
      effective = followAlias(root, aliasPath);
      if (effective == null) {
        return new Resolved(
            Types.ExternalType.of(ElasticsearchTypeConverter.ALIAS),
            comment(ElasticsearchTypeConverter.ALIAS, node, aliasPath));
      }
    }

    String esType = declaredType(effective);
    return new Resolved(toType(root, effective, esType), comment(esType, effective, aliasPath));
  }

  private static Type toType(JsonNode root, JsonNode node, String esType) {
    if (UNKNOWN_TYPE.equals(esType)) {
      return Types.ExternalType.of(UNKNOWN_TYPE);
    }

    if (ElasticsearchTypeConverter.isStructural(esType)) {
      Type struct = structOf(root, node);
      return ElasticsearchTypeConverter.NESTED.equals(esType)
          ? Types.ListType.of(struct, true)
          : struct;
    }

    return ElasticsearchTypeConverter.fromMappingType(esType);
  }

  /**
   * Builds a struct from a node's {@code properties}. An object that declares no properties has no
   * projectable shape at all, so it lands as an external type rather than an empty struct.
   */
  private static Type structOf(JsonNode root, JsonNode node) {
    JsonNode properties = properties(node);
    if (properties == null || properties.isEmpty()) {
      return Types.ExternalType.of(ElasticsearchTypeConverter.OBJECT);
    }

    List<Types.StructType.Field> fields = new ArrayList<>(properties.size());
    Iterator<Map.Entry<String, JsonNode>> entries = properties.fields();
    while (entries.hasNext()) {
      Map.Entry<String, JsonNode> entry = entries.next();
      Resolved resolved = resolve(root, entry.getValue());
      fields.add(
          Types.StructType.Field.nullableField(entry.getKey(), resolved.type, resolved.comment));
    }
    return Types.StructType.of(fields.toArray(new Types.StructType.Field[0]));
  }

  /**
   * Walks an alias to the node it points at, following a chain of aliases up to {@link
   * #MAX_ALIAS_HOPS} hops.
   *
   * @return the target node, or null if the path resolves to nothing or to another alias after the
   *     hop limit
   */
  private static JsonNode followAlias(JsonNode root, String path) {
    JsonNode target = fieldAt(root, path);
    int hops = 0;
    while (target != null
        && ElasticsearchTypeConverter.ALIAS.equals(declaredType(target))
        && hops++ < MAX_ALIAS_HOPS) {
      target = fieldAt(root, text(target, PATH));
    }

    if (target == null || ElasticsearchTypeConverter.ALIAS.equals(declaredType(target))) {
      return null;
    }
    return target;
  }

  /** Looks up a dotted field path such as {@code customer.id} from the mapping root. */
  private static JsonNode fieldAt(JsonNode root, String path) {
    if (path == null || path.isEmpty()) {
      return null;
    }

    JsonNode current = root;
    for (String segment : path.split("\\.")) {
      JsonNode properties = properties(current);
      if (properties == null) {
        return null;
      }
      current = properties.get(segment);
      if (current == null) {
        return null;
      }
    }
    return current;
  }

  /**
   * Builds the column comment, semicolon separated, omitting whatever the mapping does not declare.
   *
   * <p>For example {@code elasticsearch type: dense_vector; dims: 1024; similarity: cosine; index:
   * true}.
   */
  private static String comment(String esType, JsonNode node, String aliasPath) {
    StringBuilder comment = new StringBuilder("elasticsearch type: ").append(esType);

    append(comment, DIMS, text(node, DIMS));
    append(comment, SIMILARITY, text(node, SIMILARITY));
    append(comment, INDEX, text(node, INDEX));
    append(comment, "multi-fields", multiFields(node));
    append(comment, "alias for", aliasPath);

    return comment.toString();
  }

  /** Renders multi-fields as {@code raw (keyword), english (text)}, or null when there are none. */
  private static String multiFields(JsonNode node) {
    JsonNode fields = node == null ? null : node.get(FIELDS);
    if (fields == null || fields.isEmpty()) {
      return null;
    }

    StringBuilder rendered = new StringBuilder();
    Iterator<Map.Entry<String, JsonNode>> entries = fields.fields();
    while (entries.hasNext()) {
      Map.Entry<String, JsonNode> entry = entries.next();
      if (rendered.length() > 0) {
        rendered.append(", ");
      }
      rendered
          .append(entry.getKey())
          .append(" (")
          .append(declaredType(entry.getValue()))
          .append(')');
    }
    return rendered.toString();
  }

  private static void append(StringBuilder comment, String label, String value) {
    if (value != null && !value.isEmpty()) {
      comment.append("; ").append(label).append(": ").append(value);
    }
  }

  /**
   * Reports the mapping type of a node. A node carrying {@code properties} but no {@code type} is
   * an object, which is how Elasticsearch writes the common case.
   */
  private static String declaredType(JsonNode node) {
    if (node == null) {
      return UNKNOWN_TYPE;
    }

    JsonNode type = node.get(TYPE);
    if (type != null && type.isTextual()) {
      return type.asText();
    }
    return node.has(PROPERTIES) ? ElasticsearchTypeConverter.OBJECT : UNKNOWN_TYPE;
  }

  private static JsonNode properties(JsonNode node) {
    if (node == null) {
      return null;
    }
    JsonNode properties = node.get(PROPERTIES);
    return properties != null && properties.isObject() ? properties : null;
  }

  private static String text(JsonNode node, String field) {
    if (node == null) {
      return null;
    }
    JsonNode value = node.get(field);
    return value == null || value.isNull() ? null : value.asText();
  }

  /** A mapping node reduced to the two things a column needs. */
  private static final class Resolved {
    private final Type type;
    private final String comment;

    private Resolved(Type type, String comment) {
      this.type = type;
      this.comment = comment;
    }
  }
}
