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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.spark.connector.iceberg.IcebergPropertiesConverter.IcebergRestRouting;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergPropertiesConverter {
  private final IcebergPropertiesConverter icebergPropertiesConverter =
      IcebergPropertiesConverter.getInstance();

  @Test
  void testCatalogPropertiesWithHiveBackend() {
    Map<String, String> properties =
        icebergPropertiesConverter.toSparkCatalogProperties(
            ImmutableMap.of(
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE,
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
                "hive-uri",
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
                "hive-warehouse",
                "key1",
                "value1"));
    Assertions.assertEquals(
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED,
            "FALSE",
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "hive-uri",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "hive-warehouse"),
        properties);
  }

  @Test
  void testCatalogPropertiesWithJdbcBackend() {
    Map<String, String> properties =
        icebergPropertiesConverter.toSparkCatalogProperties(
            ImmutableMap.of(
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
                IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC,
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
                "jdbc-uri",
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
                "jdbc-warehouse",
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_USER,
                "user",
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_PASSWORD,
                "passwd",
                "key1",
                "value1"));
    Assertions.assertEquals(
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED,
            "FALSE",
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "jdbc-uri",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "jdbc-warehouse",
            IcebergPropertiesConstants.ICEBERG_CATALOG_JDBC_USER,
            "user",
            IcebergPropertiesConstants.ICEBERG_CATALOG_JDBC_PASSWORD,
            "passwd"),
        properties);
  }

  @Test
  void testCatalogPropertiesWithRestBackend() {
    Map<String, String> properties =
        icebergPropertiesConverter.toSparkCatalogProperties(
            ImmutableMap.of(
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
                IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
                "rest-uri",
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
                "rest-warehouse",
                "key1",
                "value1"));
    Assertions.assertEquals(
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED,
            "FALSE",
            IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
            IcebergPropertiesConstants.ICEBERG_CATALOG_URI,
            "rest-uri",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "rest-warehouse"),
        properties);
  }

  @Test
  void testCatalogPropertiesWithCustomBackend() {
    Map<String, String> properties =
        icebergPropertiesConverter.toSparkCatalogProperties(
            ImmutableMap.of(
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
                IcebergCatalogBackend.CUSTOM.name(),
                IcebergConstants.CATALOG_BACKEND_IMPL,
                "CustomCatalog",
                IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
                "custom-warehouse",
                "key1",
                "value1"));

    Assertions.assertEquals(
        ImmutableMap.of(
            IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED,
            "FALSE",
            IcebergPropertiesConstants.ICEBERG_CATALOG_IMPL,
            "CustomCatalog",
            IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
            "custom-warehouse"),
        properties);
  }

  // ---------------------------------------------------------------------------------------------
  // IRC routing: spark.sql.gravitino.icebergRestUri set
  // ---------------------------------------------------------------------------------------------

  private static final String IRC_URI = "http://gravitino:9001/iceberg";
  private static final String CATALOG_NAME = "sales_catalog";

  private static Map<String, String> jdbcBackedCatalogProperties() {
    return ImmutableMap.<String, String>builder()
        .put(
            IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
            IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC)
        .put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, "jdbc:mysql://mysql:3306/db")
        .put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE, "s3://bucket/wh")
        .put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_USER, "user")
        .put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_PASSWORD, "passwd")
        .put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "com.mysql.cj.jdbc.Driver")
        .put(IcebergConstants.GRAVITINO_JDBC_SCHEMA_VERSION, "V1")
        .put(IcebergConstants.CATALOG_BACKEND_NAME, "jdbc")
        .put("key1", "value1")
        .build();
  }

  private static Map<String, String> hiveBackedCatalogProperties() {
    return ImmutableMap.of(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE,
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI,
        "thrift://hms:9083",
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE,
        "s3://bucket/wh",
        "key1",
        "value1");
  }

  /**
   * The unset path must be byte-for-byte what it produces today, including the JDBC connection
   * properties that make Spark open its own connection to the metadata store.
   */
  @Test
  void testRoutingUnsetLeavesJdbcBackendUntouched() {
    Map<String, String> properties =
        icebergPropertiesConverter.toSparkCatalogProperties(jdbcBackedCatalogProperties());
    Assertions.assertEquals(
        ImmutableMap.<String, String>builder()
            .put(IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED, "FALSE")
            .put(
                IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
                IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC)
            .put(IcebergPropertiesConstants.ICEBERG_CATALOG_URI, "jdbc:mysql://mysql:3306/db")
            .put(IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE, "s3://bucket/wh")
            .put(IcebergPropertiesConstants.ICEBERG_CATALOG_JDBC_USER, "user")
            .put(IcebergPropertiesConstants.ICEBERG_CATALOG_JDBC_PASSWORD, "passwd")
            .put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "com.mysql.cj.jdbc.Driver")
            .put(IcebergConstants.ICEBERG_JDBC_SCHEMA_VERSION, "V1")
            .put(IcebergConstants.CATALOG_BACKEND_NAME, "jdbc")
            .build(),
        properties);
  }

  @Test
  void testRoutingSetForJdbcBackedCatalog() {
    Map<String, String> properties =
        IcebergPropertiesConverter.restRoutingInstance(
                IcebergRestRouting.anonymous(IRC_URI, CATALOG_NAME))
            .toSparkCatalogProperties(jdbcBackedCatalogProperties());

    Assertions.assertEquals(
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
        properties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE));
    Assertions.assertEquals(
        IRC_URI, properties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_URI));
    Assertions.assertEquals(
        CATALOG_NAME, properties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE));
    Assertions.assertEquals(
        IcebergPropertiesConverter.VENDED_CREDENTIALS,
        properties.get(IcebergPropertiesConverter.ICEBERG_REST_ACCESS_DELEGATION));
  }

  /** The backend must not change the outcome: a hive-backed catalog routes identically. */
  @Test
  void testRoutingSetForHiveBackedCatalog() {
    Map<String, String> properties =
        IcebergPropertiesConverter.restRoutingInstance(
                IcebergRestRouting.anonymous(IRC_URI, CATALOG_NAME))
            .toSparkCatalogProperties(hiveBackedCatalogProperties());

    Assertions.assertEquals(
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST,
        properties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE));
    Assertions.assertEquals(
        IRC_URI, properties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_URI));
    Assertions.assertEquals(
        CATALOG_NAME, properties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE));
  }

  /**
   * A stale backend connection property reaching the REST catalog is the most likely way to get a
   * confusing failure, so assert each one is gone rather than only that the shape is right.
   */
  @Test
  void testRoutingSetDropsBackendConnectionProperties() {
    for (Map<String, String> catalogProperties :
        ImmutableList.of(jdbcBackedCatalogProperties(), hiveBackedCatalogProperties())) {
      Map<String, String> properties =
          IcebergPropertiesConverter.restRoutingInstance(
                  IcebergRestRouting.anonymous(IRC_URI, CATALOG_NAME))
              .toSparkCatalogProperties(catalogProperties);

      // `uri` survives, but only as the IRC endpoint: both the JDBC connection string and the
      // Hive metastore URI map onto this key, so it must never carry the backend value through.
      Assertions.assertEquals(
          IRC_URI, properties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_URI));
      Assertions.assertFalse(
          properties.containsKey(IcebergPropertiesConstants.ICEBERG_CATALOG_JDBC_USER));
      Assertions.assertFalse(
          properties.containsKey(IcebergPropertiesConstants.ICEBERG_CATALOG_JDBC_PASSWORD));
      Assertions.assertFalse(properties.containsKey(IcebergConstants.GRAVITINO_JDBC_DRIVER));
      Assertions.assertFalse(properties.containsKey(IcebergConstants.ICEBERG_JDBC_SCHEMA_VERSION));
      Assertions.assertFalse(properties.containsKey(IcebergConstants.CATALOG_BACKEND));
      Assertions.assertFalse(properties.containsKey(IcebergConstants.CATALOG_BACKEND_IMPL));
      Assertions.assertFalse(properties.containsKey(IcebergConstants.CATALOG_BACKEND_NAME));
    }
  }

  /**
   * Emitting the wrong auth key sends iceberg-core to {@code <uri>/v1/oauth/tokens}, which the
   * Gravitino IRC does not serve; the failure surfaces as a 404 rather than an auth error, which is
   * easy to misdiagnose. Only the direct-bearer key is acceptable.
   */
  @Test
  void testRoutingWithBearerTokenEmitsDirectBearerKeyOnly() {
    Map<String, String> properties =
        IcebergPropertiesConverter.restRoutingInstance(
                IcebergRestRouting.withBearerToken(IRC_URI, CATALOG_NAME, "a.jwt.value"))
            .toSparkCatalogProperties(jdbcBackedCatalogProperties());

    Assertions.assertEquals(
        "a.jwt.value", properties.get(IcebergPropertiesConverter.ICEBERG_REST_TOKEN));
    assertNoTokenEndpointAuthKeys(properties);
  }

  @Test
  void testRoutingWithAuthorizationHeaderEmitsHeaderPassthroughOnly() {
    Map<String, String> properties =
        IcebergPropertiesConverter.restRoutingInstance(
                IcebergRestRouting.withAuthorizationHeader(IRC_URI, CATALOG_NAME, "Basic dXNlcjpw"))
            .toSparkCatalogProperties(jdbcBackedCatalogProperties());

    Assertions.assertEquals(
        "Basic dXNlcjpw",
        properties.get(IcebergPropertiesConverter.ICEBERG_REST_AUTHORIZATION_HEADER));
    Assertions.assertFalse(properties.containsKey(IcebergPropertiesConverter.ICEBERG_REST_TOKEN));
    assertNoTokenEndpointAuthKeys(properties);
  }

  @Test
  void testRoutingAnonymousEmitsNoAuthKeys() {
    Map<String, String> properties =
        IcebergPropertiesConverter.restRoutingInstance(
                IcebergRestRouting.anonymous(IRC_URI, CATALOG_NAME))
            .toSparkCatalogProperties(jdbcBackedCatalogProperties());

    Assertions.assertFalse(properties.containsKey(IcebergPropertiesConverter.ICEBERG_REST_TOKEN));
    Assertions.assertFalse(
        properties.containsKey(IcebergPropertiesConverter.ICEBERG_REST_AUTHORIZATION_HEADER));
    assertNoTokenEndpointAuthKeys(properties);
  }

  /**
   * iceberg-core's {@code OAuth2Manager.maybeCreateChildSession} treats {@code credential} as a
   * client_credentials fetch and each of these URNs as an RFC 8693 exchange. Both post to a token
   * endpoint the Gravitino IRC does not serve.
   */
  private static void assertNoTokenEndpointAuthKeys(Map<String, String> properties) {
    Assertions.assertFalse(
        properties.containsKey(IcebergPropertiesConverter.ICEBERG_REST_CREDENTIAL),
        "must not emit the client_credentials key");
    for (String tokenTypeUrn :
        ImmutableList.of(
            OAuth2Properties.ID_TOKEN_TYPE,
            OAuth2Properties.ACCESS_TOKEN_TYPE,
            OAuth2Properties.JWT_TOKEN_TYPE,
            OAuth2Properties.SAML1_TOKEN_TYPE,
            OAuth2Properties.SAML2_TOKEN_TYPE)) {
      Assertions.assertFalse(
          properties.containsKey(tokenTypeUrn), "must not emit token-exchange key " + tokenTypeUrn);
    }
  }
}
