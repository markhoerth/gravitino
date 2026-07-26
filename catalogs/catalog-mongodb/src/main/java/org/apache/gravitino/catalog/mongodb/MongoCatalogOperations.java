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
package org.apache.gravitino.catalog.mongodb;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.mongodb.converter.MongoSchemaConfig;
import org.apache.gravitino.catalog.mongodb.converter.MongoSchemaResolver;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read path operations for a MongoDB catalog: MongoDB databases are exposed as schemas and
 * collections as tables.
 *
 * <p>Write operations are deliberately unsupported. Creating a collection through Gravitino means
 * writing a {@code $jsonSchema} validator, which changes runtime behavior for every application
 * already writing to that database. Until that decision is settled this catalog reads only.
 */
public class MongoCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(MongoCatalogOperations.class);

  /** Databases MongoDB manages itself, which are never exposed as schemas. */
  private static final Set<String> SYSTEM_DATABASES = ImmutableSet.of("admin", "local", "config");

  private static final String WRITE_UNSUPPORTED =
      "The Gravitino MongoDB catalog is read only. Collections and databases must be managed "
          + "directly in MongoDB.";

  private MongoClient client;
  private MongoSchemaResolver resolver;

  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    String uri = conf.get(MongoCatalogPropertiesMetadata.CONNECTION_URI);
    if (uri == null || uri.trim().isEmpty()) {
      throw new IllegalArgumentException(
          MongoCatalogPropertiesMetadata.CONNECTION_URI + " is required for a MongoDB catalog");
    }

    MongoSchemaConfig schemaConfig = MongoCatalogPropertiesMetadata.toSchemaConfig(conf);
    this.resolver = new MongoSchemaResolver(schemaConfig);
    this.client = MongoClients.create(uri.trim());

    LOG.info(
        "Initialized MongoDB catalog {} with inference mode {}",
        info == null ? "unknown" : info.name(),
        schemaConfig.inferenceMode());
  }

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {
    String uri = properties.get(MongoCatalogPropertiesMetadata.CONNECTION_URI);
    if (uri == null || uri.trim().isEmpty()) {
      throw new IllegalArgumentException(
          MongoCatalogPropertiesMetadata.CONNECTION_URI + " is required for a MongoDB catalog");
    }

    try (MongoClient probe = MongoClients.create(uri.trim())) {
      probe.getDatabase("admin").runCommand(new Document("ping", 1));
    }
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    List<NameIdentifier> databases = Lists.newArrayList();
    for (String name : client.listDatabaseNames()) {
      if (!SYSTEM_DATABASES.contains(name)) {
        databases.add(NameIdentifier.of(namespace, name));
      }
    }
    return databases.toArray(new NameIdentifier[0]);
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    String schemaName = ident.name();
    if (!databaseExists(schemaName)) {
      throw new NoSuchSchemaException("MongoDB database %s does not exist", schemaName);
    }

    return MongoSchema.builder()
        .withName(schemaName)
        .withComment("MongoDB database " + schemaName)
        .withProperties(ImmutableMap.of())
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties) {
    throw new UnsupportedOperationException(WRITE_UNSUPPORTED);
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes) {
    throw new UnsupportedOperationException(WRITE_UNSUPPORTED);
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    throw new UnsupportedOperationException(WRITE_UNSUPPORTED);
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    String databaseName = NameIdentifier.of(namespace.levels()).name();
    if (!databaseExists(databaseName)) {
      throw new NoSuchSchemaException("MongoDB database %s does not exist", databaseName);
    }

    List<NameIdentifier> tables = Lists.newArrayList();
    for (String collection : client.getDatabase(databaseName).listCollectionNames()) {
      tables.add(NameIdentifier.of(namespace, collection));
    }
    return tables.toArray(new NameIdentifier[0]);
  }

  @Override
  public Table loadTable(NameIdentifier tableIdent) throws NoSuchTableException {
    String databaseName = NameIdentifier.of(tableIdent.namespace().levels()).name();
    String collectionName = tableIdent.name();

    if (!collectionExists(databaseName, collectionName)) {
      throw new NoSuchTableException(
          "MongoDB collection %s.%s does not exist", databaseName, collectionName);
    }

    MongoDatabase database = client.getDatabase(databaseName);
    Column[] columns = resolver.resolveColumns(database, collectionName);

    return MongoTable.builder()
        .withName(collectionName)
        .withDatabaseName(databaseName)
        .withComment("MongoDB collection " + databaseName + "." + collectionName)
        .withColumns(columns)
        .withProperties(collectionProperties(database, collectionName))
        .withIndexes(primaryKeyOnId())
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes) {
    throw new UnsupportedOperationException(WRITE_UNSUPPORTED);
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes) {
    throw new UnsupportedOperationException(WRITE_UNSUPPORTED);
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    throw new UnsupportedOperationException(WRITE_UNSUPPORTED);
  }

  @Override
  public void close() {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  private boolean databaseExists(String databaseName) {
    if (SYSTEM_DATABASES.contains(databaseName)) {
      return false;
    }
    for (String name : client.listDatabaseNames()) {
      if (name.equals(databaseName)) {
        return true;
      }
    }
    return false;
  }

  private boolean collectionExists(String databaseName, String collectionName) {
    if (!databaseExists(databaseName)) {
      return false;
    }
    for (String name : client.getDatabase(databaseName).listCollectionNames()) {
      if (name.equals(collectionName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Every MongoDB document carries an {@code _id} that is unique and indexed, which is the closest
   * thing the model has to a primary key.
   */
  private Index[] primaryKeyOnId() {
    return new Index[] {
      Indexes.primary("_id_", new String[][] {new String[] {MongoSchemaResolver.ID_FIELD}})
    };
  }

  private Map<String, String> collectionProperties(MongoDatabase database, String collectionName) {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

    Document validator = resolver.readJsonSchemaValidator(database, collectionName);
    properties.put(MongoTablePropertiesMetadata.HAS_VALIDATOR, Boolean.toString(validator != null));

    Document info = database.listCollections().filter(new Document("name", collectionName)).first();
    if (info != null) {
      Document options = info.get("options", Document.class);
      if (options != null) {
        properties.put(
            MongoTablePropertiesMetadata.CAPPED,
            Boolean.toString(options.getBoolean("capped", false)));

        String level = options.getString("validationLevel");
        if (level != null) {
          properties.put(MongoTablePropertiesMetadata.VALIDATION_LEVEL, level);
        }

        String action = options.getString("validationAction");
        if (action != null) {
          properties.put(MongoTablePropertiesMetadata.VALIDATION_ACTION, action);
        }
      }
    }

    return properties.build();
  }
}
