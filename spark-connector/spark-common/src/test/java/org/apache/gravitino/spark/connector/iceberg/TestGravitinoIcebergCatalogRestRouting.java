/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.spark.connector.iceberg;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.spark.connector.iceberg.IcebergPropertiesConverter.IcebergRestRouting;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

/**
 * Verifies that routing through the Gravitino IRC suppresses the catalog-level credential fetch.
 *
 * <p>The IRC vends per table on the loadTable response; a catalog-level credential fetched
 * alongside would leave Iceberg holding two sets of keys with no defined precedence. For the {@code
 * s3-token} provider the catalog-scoped call also returns nothing at all, because {@code
 * S3TokenGenerator.generate()} requires a table path.
 */
@TestInstance(Lifecycle.PER_CLASS)
public class TestGravitinoIcebergCatalogRestRouting {

  private static final String IRC_URI = "http://gravitino:9001/iceberg";
  private static final String CATALOG_NAME = "sales_catalog";

  private static final Map<String, String> JDBC_BACKED_PROPERTIES =
      ImmutableMap.of(
          IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
          IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC,
          IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
          "jdbc:mysql://mysql:3306/db",
          IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
          "s3://bucket/wh");

  /** Exposes the protected client field so a test can supply a mock without a live server. */
  private static final class StubbedIcebergCatalog extends GravitinoIcebergCatalog {
    private StubbedIcebergCatalog(Catalog catalogClient) {
      this.gravitinoCatalogClient = catalogClient;
    }
  }

  @BeforeAll
  void initCatalogManager() {
    // BaseCatalog's constructor calls GravitinoCatalogManager.get().
    GravitinoCatalogManager.create(() -> mock(GravitinoClient.class));
  }

  @AfterAll
  void cleanupCatalogManager() {
    GravitinoCatalogManager.get().close();
  }

  @Test
  void testRoutingSkipsCatalogLevelCredentialFetch() {
    Catalog catalogClient = mock(Catalog.class);
    StubbedIcebergCatalog catalog = new StubbedIcebergCatalog(catalogClient);

    Map<String, String> properties =
        catalog.buildSparkCatalogProperties(
            new CaseInsensitiveStringMap(ImmutableMap.of()),
            JDBC_BACKED_PROPERTIES,
            Optional.of(IcebergRestRouting.anonymous(IRC_URI, CATALOG_NAME)));

    verify(catalogClient, never()).supportsCredentials();
    Assertions.assertEquals(
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
        properties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE));
  }

  @Test
  void testCredentialFetchStillHappensWhenRoutingUnset() {
    Catalog catalogClient = mock(Catalog.class);
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    when(catalogClient.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenReturn(new Credential[0]);
    StubbedIcebergCatalog catalog = new StubbedIcebergCatalog(catalogClient);

    Map<String, String> properties =
        catalog.buildSparkCatalogProperties(
            new CaseInsensitiveStringMap(ImmutableMap.of()),
            JDBC_BACKED_PROPERTIES,
            Optional.empty());

    verify(catalogClient).supportsCredentials();
    Assertions.assertEquals(
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC,
        properties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE));
  }
}
