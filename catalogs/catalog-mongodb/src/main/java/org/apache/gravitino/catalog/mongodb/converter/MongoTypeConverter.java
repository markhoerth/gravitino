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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;

/**
 * Converts between BSON types and Gravitino types.
 *
 * <p>Three directions are supported: a concrete BSON value observed during sampling, a {@code
 * $jsonSchema} validator node, and a Gravitino type rendered back into a validator node.
 */
public final class MongoTypeConverter {

  /**
   * BSON {@code objectId} has no Gravitino equivalent, so it is carried as an external type. The
   * mapping is lossless in both directions, which keeps an alterTable from rewriting an existing
   * {@code _id} into a string.
   */
  public static final String OBJECT_ID_TYPE = "objectId";

  private final MongoSchemaConfig config;

  public MongoTypeConverter(MongoSchemaConfig config) {
    this.config = config;
  }

  /**
   * Derives a Gravitino type from a concrete BSON value.
   *
   * @param value the observed value
   * @param depth current nesting depth, starting at 1 for a top level field
   * @return the corresponding Gravitino type
   */
  public Type fromValue(BsonValue value, int depth) {
    if (value == null || value.isNull()) {
      return Types.NullType.get();
    }

    switch (value.getBsonType()) {
      case BOOLEAN:
        return Types.BooleanType.get();
      case INT32:
        return Types.IntegerType.get();
      case INT64:
        return Types.LongType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case DECIMAL128:
        return Types.DecimalType.of(MongoSchemaConfig.DECIMAL128_PRECISION, config.decimalScale());
      case STRING:
      case REGULAR_EXPRESSION:
      case JAVASCRIPT:
      case JAVASCRIPT_WITH_SCOPE:
      case SYMBOL:
        return Types.StringType.get();
      case OBJECT_ID:
        return Types.ExternalType.of(OBJECT_ID_TYPE);
      case DATE_TIME:
      case TIMESTAMP:
        return Types.TimestampType.withTimeZone();
      case BINARY:
        return Types.BinaryType.get();
      case DOCUMENT:
        return structFromDocument(value.asDocument(), depth);
      case ARRAY:
        return listFromArray(value.asArray(), depth);
      default:
        return Types.ExternalType.of(value.getBsonType().name().toLowerCase());
    }
  }

  private Type structFromDocument(BsonDocument document, int depth) {
    if (depth >= config.maxNestingDepth() || document.isEmpty()) {
      return Types.StringType.get();
    }

    List<Types.StructType.Field> fields = new ArrayList<>(document.size());
    for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
      Type fieldType = fromValue(entry.getValue(), depth + 1);
      fields.add(Types.StructType.Field.nullableField(entry.getKey(), fieldType));
    }
    return Types.StructType.of(fields.toArray(new Types.StructType.Field[0]));
  }

  private Type listFromArray(BsonArray array, int depth) {
    if (depth >= config.maxNestingDepth() || array.isEmpty()) {
      return Types.ListType.of(Types.StringType.get(), true);
    }

    Type elementType = fromValue(array.get(0), depth + 1);
    for (int i = 1; i < array.size(); i++) {
      elementType = unify(elementType, fromValue(array.get(i), depth + 1));
    }
    return Types.ListType.of(elementType, true);
  }

  /**
   * Derives a Gravitino type from a {@code $jsonSchema} property node.
   *
   * @param node the schema node, expected to carry a {@code bsonType} entry
   * @param depth current nesting depth, starting at 1 for a top level field
   * @return the corresponding Gravitino type
   */
  @SuppressWarnings("unchecked")
  public Type fromSchemaNode(Document node, int depth) {
    List<String> bsonTypes = readBsonTypes(node);
    if (bsonTypes.isEmpty()) {
      return Types.StringType.get();
    }

    List<String> concrete = new ArrayList<>(bsonTypes);
    concrete.remove("null");
    if (concrete.isEmpty()) {
      return Types.NullType.get();
    }

    Type resolved = fromSchemaType(concrete.get(0), node, depth);
    for (int i = 1; i < concrete.size(); i++) {
      resolved = unify(resolved, fromSchemaType(concrete.get(i), node, depth));
    }
    return resolved;
  }

  @SuppressWarnings("unchecked")
  private Type fromSchemaType(String bsonType, Document node, int depth) {
    switch (bsonType) {
      case "bool":
        return Types.BooleanType.get();
      case "int":
        return Types.IntegerType.get();
      case "long":
        return Types.LongType.get();
      case "double":
        return Types.DoubleType.get();
      case "number":
        return Types.DoubleType.get();
      case "decimal":
        return Types.DecimalType.of(MongoSchemaConfig.DECIMAL128_PRECISION, config.decimalScale());
      case "string":
      case "regex":
      case "javascript":
      case "symbol":
        return Types.StringType.get();
      case "objectId":
        return Types.ExternalType.of(OBJECT_ID_TYPE);
      case "date":
      case "timestamp":
        return Types.TimestampType.withTimeZone();
      case "binData":
        return Types.BinaryType.get();
      case "object":
        return structFromSchemaNode(node, depth);
      case "array":
        return listFromSchemaNode(node, depth);
      default:
        return Types.ExternalType.of(bsonType);
    }
  }

  @SuppressWarnings("unchecked")
  private Type structFromSchemaNode(Document node, int depth) {
    Document properties = node.get("properties", Document.class);
    if (depth >= config.maxNestingDepth() || properties == null || properties.isEmpty()) {
      return Types.StringType.get();
    }

    List<String> required =
        (List<String>) node.getOrDefault("required", Collections.<String>emptyList());

    List<Types.StructType.Field> fields = new ArrayList<>(properties.size());
    for (String name : properties.keySet()) {
      Document child = properties.get(name, Document.class);
      Type fieldType = fromSchemaNode(child, depth + 1);
      boolean nullable = !required.contains(name) || readBsonTypes(child).contains("null");
      fields.add(
          nullable
              ? Types.StructType.Field.nullableField(name, fieldType)
              : Types.StructType.Field.notNullField(name, fieldType));
    }
    return Types.StructType.of(fields.toArray(new Types.StructType.Field[0]));
  }

  private Type listFromSchemaNode(Document node, int depth) {
    Document items = node.get("items", Document.class);
    if (depth >= config.maxNestingDepth() || items == null) {
      return Types.ListType.of(Types.StringType.get(), true);
    }
    return Types.ListType.of(fromSchemaNode(items, depth + 1), true);
  }

  /**
   * Renders a Gravitino type as a {@code $jsonSchema} property node, used when Gravitino creates or
   * alters a collection.
   *
   * @param type the Gravitino type
   * @param nullable whether {@code null} is an accepted value
   * @return a schema node carrying at least a {@code bsonType} entry
   */
  public Document toSchemaNode(Type type, boolean nullable) {
    Document node = new Document();

    if (type instanceof Types.StructType) {
      Types.StructType struct = (Types.StructType) type;
      Document properties = new Document();
      List<String> required = new ArrayList<>();
      for (Types.StructType.Field field : struct.fields()) {
        properties.put(field.name(), toSchemaNode(field.type(), field.nullable()));
        if (!field.nullable()) {
          required.add(field.name());
        }
      }
      node.put("bsonType", bsonTypeNames("object", nullable));
      node.put("properties", properties);
      if (!required.isEmpty()) {
        node.put("required", required);
      }
      node.put("additionalProperties", config.allowAdditionalProperties());
      return node;
    }

    if (type instanceof Types.ListType) {
      Types.ListType list = (Types.ListType) type;
      node.put("bsonType", bsonTypeNames("array", nullable));
      node.put("items", toSchemaNode(list.elementType(), list.elementNullable()));
      return node;
    }

    node.put("bsonType", bsonTypeNames(scalarBsonName(type), nullable));
    return node;
  }

  private String scalarBsonName(Type type) {
    if (type instanceof Types.ExternalType) {
      return ((Types.ExternalType) type).catalogString();
    }
    if (type instanceof Types.BooleanType) {
      return "bool";
    } else if (type instanceof Types.ByteType
        || type instanceof Types.ShortType
        || type instanceof Types.IntegerType) {
      return "int";
    } else if (type instanceof Types.LongType) {
      return "long";
    } else if (type instanceof Types.FloatType || type instanceof Types.DoubleType) {
      return "double";
    } else if (type instanceof Types.DecimalType) {
      return "decimal";
    } else if (type instanceof Types.TimestampType || type instanceof Types.DateType) {
      return "date";
    } else if (type instanceof Types.BinaryType || type instanceof Types.FixedType) {
      return "binData";
    } else if (type instanceof Types.NullType) {
      return "null";
    }
    return "string";
  }

  private Object bsonTypeNames(String name, boolean nullable) {
    if (!nullable || "null".equals(name)) {
      return name;
    }
    List<String> names = new ArrayList<>(2);
    names.add(name);
    names.add("null");
    return names;
  }

  @SuppressWarnings("unchecked")
  private ImmutableList<String> readBsonTypes(Document node) {
    if (node == null) {
      return ImmutableList.of();
    }
    Object raw = node.get("bsonType");
    if (raw == null) {
      raw = node.get("type");
    }
    if (raw instanceof String) {
      return ImmutableList.of((String) raw);
    }
    if (raw instanceof List) {
      return ImmutableList.copyOf((List<String>) raw);
    }
    return ImmutableList.of();
  }

  /**
   * Merges two observations of the same field path into a single type.
   *
   * <p>Numeric observations widen, {@code null} is absorbed, structs merge field by field, and
   * anything else falls through to the configured conflict policy.
   *
   * @param left the type resolved so far
   * @param right a newly observed type
   * @return the merged type
   */
  public Type unify(Type left, Type right) {
    if (left == null) {
      return right;
    }
    if (right == null || left.equals(right)) {
      return left;
    }
    if (left instanceof Types.NullType) {
      return right;
    }
    if (right instanceof Types.NullType) {
      return left;
    }

    Type widened = widenNumeric(left, right);
    if (widened != null) {
      return widened;
    }

    if (left instanceof Types.StructType && right instanceof Types.StructType) {
      return mergeStructs((Types.StructType) left, (Types.StructType) right);
    }

    if (left instanceof Types.ListType && right instanceof Types.ListType) {
      Type element =
          unify(((Types.ListType) left).elementType(), ((Types.ListType) right).elementType());
      return Types.ListType.of(element, true);
    }

    return resolveConflict(left, right);
  }

  private Type widenNumeric(Type left, Type right) {
    int leftRank = numericRank(left);
    int rightRank = numericRank(right);
    if (leftRank == 0 || rightRank == 0) {
      return null;
    }
    return leftRank >= rightRank ? left : right;
  }

  private int numericRank(Type type) {
    if (type instanceof Types.IntegerType) {
      return 1;
    } else if (type instanceof Types.LongType) {
      return 2;
    } else if (type instanceof Types.DoubleType) {
      return 3;
    } else if (type instanceof Types.DecimalType) {
      return 4;
    }
    return 0;
  }

  private Type mergeStructs(Types.StructType left, Types.StructType right) {
    Map<String, Types.StructType.Field> merged = new LinkedHashMap<>();
    for (Types.StructType.Field field : left.fields()) {
      merged.put(field.name(), field);
    }

    for (Types.StructType.Field field : right.fields()) {
      Types.StructType.Field existing = merged.get(field.name());
      if (existing == null) {
        merged.put(field.name(), Types.StructType.Field.nullableField(field.name(), field.type()));
      } else {
        Type unified = unify(existing.type(), field.type());
        merged.put(field.name(), Types.StructType.Field.nullableField(field.name(), unified));
      }
    }

    // A field absent from either side is optional in the merged view.
    for (Types.StructType.Field field : left.fields()) {
      boolean presentOnRight = false;
      for (Types.StructType.Field candidate : right.fields()) {
        if (candidate.name().equals(field.name())) {
          presentOnRight = true;
          break;
        }
      }
      if (!presentOnRight) {
        Types.StructType.Field current = merged.get(field.name());
        merged.put(
            field.name(), Types.StructType.Field.nullableField(field.name(), current.type()));
      }
    }

    return Types.StructType.of(merged.values().toArray(new Types.StructType.Field[0]));
  }

  private Type resolveConflict(Type left, Type right) {
    switch (config.conflictPolicy()) {
      case WIDEN_TO_STRING:
        return Types.StringType.get();
      case EXTERNAL_TYPE:
        return Types.ExternalType.of(left.simpleString() + "|" + right.simpleString());
      case FAIL:
      default:
        throw new IllegalStateException(
            "Incompatible BSON types observed for the same field: "
                + left.simpleString()
                + " and "
                + right.simpleString()
                + ". Set "
                + MongoSchemaConfig.CONFLICT_POLICY
                + " to widen-to-string or external-type to tolerate this.");
    }
  }
}
