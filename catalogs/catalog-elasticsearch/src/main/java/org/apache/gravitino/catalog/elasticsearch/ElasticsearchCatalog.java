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

import java.util.Map;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;

/**
 * A read only relational catalog over Elasticsearch, exposing indices as tables whose columns come
 * from the index mapping.
 */
public class ElasticsearchCatalog extends BaseCatalog<ElasticsearchCatalog> {

  static final ElasticsearchCatalogPropertiesMetadata CATALOG_PROPERTIES_METADATA =
      new ElasticsearchCatalogPropertiesMetadata();
  static final ElasticsearchSchemaPropertiesMetadata SCHEMA_PROPERTIES_METADATA =
      new ElasticsearchSchemaPropertiesMetadata();
  static final ElasticsearchTablePropertiesMetadata TABLE_PROPERTIES_METADATA =
      new ElasticsearchTablePropertiesMetadata();

  @Override
  public String shortName() {
    return "elasticsearch";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    return new ElasticsearchCatalogOperations();
  }

  @Override
  protected Capability newCapability() {
    return new ElasticsearchCatalogCapability();
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
