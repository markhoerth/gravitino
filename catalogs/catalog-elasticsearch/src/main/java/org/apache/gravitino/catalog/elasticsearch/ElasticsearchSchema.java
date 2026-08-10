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

import org.apache.gravitino.connector.BaseSchema;

/**
 * The single fixed schema this catalog exposes. Elasticsearch has no level between a cluster and an
 * index, so one is invented to satisfy Gravitino's three level namespace.
 */
public class ElasticsearchSchema extends BaseSchema {

  private ElasticsearchSchema() {}

  /** A builder for {@link ElasticsearchSchema}. */
  public static class Builder extends BaseSchemaBuilder<Builder, ElasticsearchSchema> {
    private Builder() {}

    @Override
    protected ElasticsearchSchema internalBuild() {
      ElasticsearchSchema schema = new ElasticsearchSchema();
      schema.name = name;
      schema.comment = comment;
      schema.properties = properties;
      schema.auditInfo = auditInfo;
      return schema;
    }
  }

  /**
   * Creates a builder.
   *
   * @return a new builder instance
   */
  public static Builder builder() {
    return new Builder();
  }
}
