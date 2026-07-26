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

import java.util.Map;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;

/**
 * A relational catalog over MongoDB, exposing databases as schemas and collections as tables. Only
 * useful where collections hold documents of a consistent shape.
 */
public class MongoCatalog extends BaseCatalog<MongoCatalog> {

  static final MongoCatalogPropertiesMetadata CATALOG_PROPERTIES_METADATA =
      new MongoCatalogPropertiesMetadata();
  static final MongoSchemaPropertiesMetadata SCHEMA_PROPERTIES_METADATA =
      new MongoSchemaPropertiesMetadata();
  static final MongoTablePropertiesMetadata TABLE_PROPERTIES_METADATA =
      new MongoTablePropertiesMetadata();

  @Override
  public String shortName() {
    return "mongodb";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    return new MongoCatalogOperations();
  }

  @Override
  protected Capability newCapability() {
    return new MongoCatalogCapability();
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return CATALOG_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return SCHEMA_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return TABLE_PROPERTIES_METADATA;
  }
}
