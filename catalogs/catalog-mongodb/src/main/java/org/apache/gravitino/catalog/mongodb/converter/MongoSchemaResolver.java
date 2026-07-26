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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;

/**
 * Derives the column list for a MongoDB collection.
 *
 * <p>A {@code $jsonSchema} validator is authoritative when present, because it makes repeated loads
 * of the same collection return identical columns. Sampling is a fallback for collections that
 * carry no validator, and its result is stable only to the extent that the collection's documents
 * are.
 */
public final class MongoSchemaResolver {

  /** Every MongoDB document carries an {@code _id}, which becomes the primary key column. */
  public static final String ID_FIELD = "_id";

  private static final int TOP_LEVEL_DEPTH = 1;

  private final MongoSchemaConfig config;
  private final MongoTypeConverter converter;

  public MongoSchemaResolver(MongoSchemaConfig config) {
    this.config = config;
    this.converter = new MongoTypeConverter(config);
  }

  /**
   * Resolves the columns of a collection.
   *
   * @param database the database holding the collection
   * @param collectionName the collection name
   * @return the resolved columns, with {@code _id} first
   */
  public Column[] resolveColumns(MongoDatabase database, String collectionName) {
    Document validator = readJsonSchemaValidator(database, collectionName);

    switch (config.inferenceMode()) {
      case VALIDATOR:
        if (validator == null) {
          throw new IllegalStateException(
              "Collection "
                  + database.getName()
                  + "."
                  + collectionName
                  + " has no $jsonSchema validator and "
                  + MongoSchemaConfig.INFERENCE_MODE
                  + " is set to validator. Set it to validator-then-sample to allow sampling.");
        }
        return fromValidator(validator);

      case SAMPLE:
        return fromSample(database.getCollection(collectionName, BsonDocument.class));

      case VALIDATOR_THEN_SAMPLE:
      default:
        return validator != null
            ? fromValidator(validator)
            : fromSample(database.getCollection(collectionName, BsonDocument.class));
    }
  }

  /**
   * Reads the {@code $jsonSchema} node out of a collection's validator, if it has one.
   *
   * @param database the database holding the collection
   * @param collectionName the collection name
   * @return the {@code $jsonSchema} node, or null when the collection has no schema validator
   */
  public Document readJsonSchemaValidator(MongoDatabase database, String collectionName) {
    Document collectionInfo =
        database.listCollections().filter(Filters.eq("name", collectionName)).first();
    if (collectionInfo == null) {
      return null;
    }

    Document options = collectionInfo.get("options", Document.class);
    if (options == null) {
      return null;
    }

    Document validator = options.get("validator", Document.class);
    if (validator == null) {
      return null;
    }

    return validator.get("$jsonSchema", Document.class);
  }

  @SuppressWarnings("unchecked")
  private Column[] fromValidator(Document jsonSchema) {
    Document properties = jsonSchema.get("properties", Document.class);
    if (properties == null || properties.isEmpty()) {
      return new Column[] {idColumn(Types.ExternalType.of(MongoTypeConverter.OBJECT_ID_TYPE))};
    }

    List<String> required =
        (List<String>) jsonSchema.getOrDefault("required", Collections.<String>emptyList());

    Map<String, Column> columns = new LinkedHashMap<>();
    for (String name : properties.keySet()) {
      Document node = properties.get(name, Document.class);
      Type type = converter.fromSchemaNode(node, TOP_LEVEL_DEPTH);
      boolean nullable = !required.contains(name);
      String comment = node == null ? null : node.getString("description");
      columns.put(name, column(name, type, comment, nullable));
    }

    return orderWithIdFirst(columns);
  }

  private Column[] fromSample(MongoCollection<BsonDocument> collection) {
    Map<String, Type> types = new LinkedHashMap<>();
    Map<String, Integer> occurrences = new LinkedHashMap<>();
    Set<String> fieldOrder = new LinkedHashSet<>();
    int sampled = 0;

    try (MongoCursor<BsonDocument> cursor =
        collection
            .aggregate(Collections.singletonList(Aggregates.sample(config.sampleSize())))
            .iterator()) {
      while (cursor.hasNext()) {
        BsonDocument document = cursor.next();
        sampled++;
        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
          String name = entry.getKey();
          fieldOrder.add(name);
          Type observed = converter.fromValue(entry.getValue(), TOP_LEVEL_DEPTH);
          types.merge(name, observed, converter::unify);
          occurrences.merge(name, 1, Integer::sum);
        }
      }
    }

    if (sampled == 0) {
      return new Column[] {idColumn(Types.ExternalType.of(MongoTypeConverter.OBJECT_ID_TYPE))};
    }

    Map<String, Column> columns = new LinkedHashMap<>();
    for (String name : fieldOrder) {
      Type type = types.get(name);
      // Sampling observes presence, never absence: a field seen in every sampled document may
      // still be missing elsewhere in the collection. Only a validator's required list can
      // justify NOT NULL, so an inferred column is always nullable. The presence ratio is
      // reported in the comment instead.
      boolean nullable = true;
      if (type instanceof Types.NullType) {
        type = Types.StringType.get();
      }
      String comment =
          String.format("Inferred from %d of %d sampled documents", occurrences.get(name), sampled);
      columns.put(name, column(name, type, comment, nullable));
    }

    return orderWithIdFirst(columns);
  }

  private Column[] orderWithIdFirst(Map<String, Column> columns) {
    List<Column> ordered = new ArrayList<>(columns.size() + 1);

    Column id = columns.remove(ID_FIELD);
    ordered.add(
        id != null
            ? column(ID_FIELD, id.dataType(), id.comment(), false)
            : idColumn(Types.ExternalType.of(MongoTypeConverter.OBJECT_ID_TYPE)));

    // MongoDB documents carry no column order, and $sample returns a different draw every call,
    // so first-seen order would reshuffle the schema between loads. Sorting by name after _id
    // makes repeated loads of the same collection agree.
    List<Column> rest = new ArrayList<>(columns.values());
    rest.sort(Comparator.comparing(Column::name));
    ordered.addAll(rest);

    return ordered.toArray(new Column[0]);
  }

  private Column idColumn(Type type) {
    return column(ID_FIELD, type, "MongoDB document identifier", false);
  }

  private Column column(String name, Type type, String comment, boolean nullable) {
    return Column.of(name, type, comment, nullable, false, Column.DEFAULT_VALUE_NOT_SET);
  }
}
