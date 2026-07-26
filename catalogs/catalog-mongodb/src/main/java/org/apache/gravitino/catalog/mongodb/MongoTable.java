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

import com.google.common.collect.Maps;
import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.TableOperations;

/** Represents a MongoDB collection as a Gravitino table. */
public class MongoTable extends BaseTable {

  private String databaseName;

  private MongoTable() {}

  @Override
  protected TableOperations newOps() {
    throw new UnsupportedOperationException(
        "MongoDB catalog does not support partition operations on collection: " + name);
  }

  /**
   * Returns the MongoDB database holding this collection.
   *
   * @return the database name
   */
  public String databaseName() {
    return databaseName;
  }

  /** A builder for {@link MongoTable}. */
  public static class Builder extends BaseTableBuilder<Builder, MongoTable> {
    private String databaseName;

    private Builder() {}

    /**
     * Sets the database holding the collection.
     *
     * @param databaseName the database name
     * @return this builder
     */
    public Builder withDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    @Override
    protected MongoTable internalBuild() {
      MongoTable table = new MongoTable();
      table.name = name;
      table.comment = comment;
      table.properties = properties != null ? Maps.newHashMap(properties) : Maps.newHashMap();
      table.auditInfo = auditInfo;
      table.columns = columns;
      table.partitioning = partitioning;
      table.distribution = distribution;
      table.sortOrders = sortOrders;
      table.indexes = indexes;
      table.proxyPlugin = proxyPlugin;
      table.databaseName = databaseName;
      return table;
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
