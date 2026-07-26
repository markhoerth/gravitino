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

import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.rel.Column;
import org.bson.Document;

/**
 * Builds the {@code $jsonSchema} validator that Gravitino writes when it creates or alters a
 * collection.
 *
 * <p>Writing a validator on create is what makes a Gravitino managed collection round trip: the
 * columns handed to {@code createTable} are the columns a later {@code loadTable} returns, with no
 * dependence on the documents that happen to be present.
 */
public final class MongoValidatorBuilder {

  private final MongoSchemaConfig config;
  private final MongoTypeConverter converter;

  public MongoValidatorBuilder(MongoSchemaConfig config) {
    this.config = config;
    this.converter = new MongoTypeConverter(config);
  }

  /**
   * Builds the {@code $jsonSchema} node describing a collection.
   *
   * @param columns the Gravitino column definitions
   * @return the schema node, suitable for wrapping in a validator
   */
  public Document buildJsonSchema(Column[] columns) {
    Document properties = new Document();
    List<String> required = new ArrayList<>();

    for (Column column : columns) {
      Document node = converter.toSchemaNode(column.dataType(), column.nullable());
      if (column.comment() != null && !column.comment().isEmpty()) {
        node.put("description", column.comment());
      }
      properties.put(column.name(), node);

      if (!column.nullable()) {
        required.add(column.name());
      }
    }

    Document schema = new Document("bsonType", "object").append("properties", properties);
    if (!required.isEmpty()) {
      schema.append("required", required);
    }
    schema.append("additionalProperties", config.allowAdditionalProperties());
    return schema;
  }

  /**
   * Wraps a schema node in the validator document accepted by {@code createCollection} and {@code
   * collMod}.
   *
   * @param columns the Gravitino column definitions
   * @return the validator document
   */
  public Document buildValidator(Column[] columns) {
    return new Document("$jsonSchema", buildJsonSchema(columns));
  }

  /**
   * Builds the {@code collMod} command that replaces a collection's validator in place, which is
   * how column additions, drops, and type changes are applied.
   *
   * @param collectionName the collection to modify
   * @param columns the full column list after the alteration
   * @param validationLevel one of {@code off}, {@code moderate}, or {@code strict}
   * @param validationAction one of {@code error} or {@code warn}
   * @return the command document
   */
  public Document buildCollMod(
      String collectionName, Column[] columns, String validationLevel, String validationAction) {
    return new Document("collMod", collectionName)
        .append("validator", buildValidator(columns))
        .append("validationLevel", validationLevel)
        .append("validationAction", validationAction);
  }
}
