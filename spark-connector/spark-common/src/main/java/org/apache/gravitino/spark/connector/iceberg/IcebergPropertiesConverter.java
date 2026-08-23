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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.iceberg.rest.auth.OAuth2Properties;

/** Transform Apache Iceberg catalog properties between Apache Spark and Apache Gravitino. */
public class IcebergPropertiesConverter implements PropertiesConverter {

  /**
   * Iceberg's direct-bearer catalog property. A value here is sent as {@code Authorization: Bearer}
   * with no token-endpoint round trip; see {@link IcebergRestRouting} for why that matters.
   */
  @VisibleForTesting static final String ICEBERG_REST_TOKEN = OAuth2Properties.TOKEN;

  /**
   * Iceberg's client-credentials catalog property. Named here only so tests can assert it is never
   * emitted.
   */
  @VisibleForTesting static final String ICEBERG_REST_CREDENTIAL = OAuth2Properties.CREDENTIAL;

  /** Generic {@code header.} passthrough for the HTTP Authorization header. */
  @VisibleForTesting static final String ICEBERG_REST_AUTHORIZATION_HEADER = "header.Authorization";

  /**
   * Requests per-table credential vending. Iceberg sends no access-delegation header of its own and
   * exposes no property for it in any pinned version, so the {@code header.} passthrough is the
   * only way to ask for it.
   */
  @VisibleForTesting
  static final String ICEBERG_REST_ACCESS_DELEGATION = IcebergConstants.ICEBERG_ACCESS_DELEGATION;

  /** The only access-delegation mode the Gravitino IRC vends credentials for. */
  @VisibleForTesting static final String VENDED_CREDENTIALS = "vended-credentials";

  @Nullable private final IcebergRestRouting restRouting;

  public static class IcebergPropertiesConverterHolder {
    private static final IcebergPropertiesConverter INSTANCE = new IcebergPropertiesConverter(null);
  }

  private IcebergPropertiesConverter(@Nullable IcebergRestRouting restRouting) {
    this.restRouting = restRouting;
  }

  public static IcebergPropertiesConverter getInstance() {
    return IcebergPropertiesConverter.IcebergPropertiesConverterHolder.INSTANCE;
  }

  /**
   * Returns a converter that routes Iceberg table loads through the Gravitino IRC.
   *
   * @param restRouting the IRC endpoint and authentication to use.
   * @return a converter emitting REST catalog properties.
   */
  public static IcebergPropertiesConverter restRoutingInstance(IcebergRestRouting restRouting) {
    Preconditions.checkArgument(restRouting != null, "restRouting should not be null");
    return new IcebergPropertiesConverter(restRouting);
  }

  @Override
  public Map<String, String> toSparkCatalogProperties(Map<String, String> properties) {
    Preconditions.checkArgument(
        properties != null, "Iceberg Catalog properties should not be null");
    if (restRouting != null) {
      return toRestCatalogProperties(properties);
    }
    Map<String, String> all = IcebergPropertiesUtils.toIcebergCatalogProperties(properties);
    String catalogBackend = all.remove(IcebergConstants.CATALOG_BACKEND);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogBackend),
        String.format("%s should not be empty", IcebergConstants.CATALOG_BACKEND));
    if (catalogBackend.equalsIgnoreCase(IcebergCatalogBackend.CUSTOM.name())) {
      String catalogBackendImpl = all.remove(IcebergConstants.CATALOG_BACKEND_IMPL);
      Preconditions.checkArgument(
          StringUtils.isNotBlank(catalogBackendImpl),
          String.format(
              "%s should not be empty when %s is %s",
              IcebergConstants.CATALOG_BACKEND_IMPL,
              IcebergConstants.CATALOG_BACKEND,
              IcebergCatalogBackend.CUSTOM.name()));
      all.put(IcebergPropertiesConstants.ICEBERG_CATALOG_IMPL, catalogBackendImpl);
    } else {
      all.put(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE, catalogBackend);
    }

    all.put(IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED, "FALSE");
    return all;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  /**
   * Builds Iceberg REST catalog properties pointing at the Gravitino IRC.
   *
   * <p>{@code catalog-backend} is deliberately not consulted. It describes where Gravitino stores
   * catalog metadata, not how the engine should reach tables, so every {@code lakehouse-iceberg}
   * catalog takes this path once routing is enabled.
   */
  private Map<String, String> toRestCatalogProperties(Map<String, String> properties) {
    Map<String, String> all = IcebergPropertiesUtils.toIcebergCatalogProperties(properties);

    // The engine no longer talks to the metadata backend, so nothing describing that backend may
    // reach the REST catalog. A stale `uri` in particular is the most likely source of a confusing
    // failure: both the JDBC connection string and the Hive metastore URI map onto this one key.
    all.remove(IcebergConstants.URI);
    all.remove(IcebergConstants.GRAVITINO_JDBC_DRIVER);
    all.remove(IcebergConstants.ICEBERG_JDBC_USER);
    all.remove(IcebergConstants.ICEBERG_JDBC_PASSWORD);
    all.remove(IcebergConstants.ICEBERG_JDBC_SCHEMA_VERSION);
    all.remove(IcebergConstants.CATALOG_BACKEND);
    all.remove(IcebergConstants.CATALOG_BACKEND_IMPL);
    all.remove(IcebergConstants.CATALOG_BACKEND_NAME);

    all.put(
        IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST);
    all.put(IcebergPropertiesConstants.ICEBERG_CATALOG_URI, restRouting.restUri());
    // The IRC addresses catalogs by their Gravitino name: GET /iceberg/v1/config?warehouse=<name>
    // answers with defaults.prefix = <name>.
    all.put(IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE, restRouting.catalogName());
    all.put(ICEBERG_REST_ACCESS_DELEGATION, VENDED_CREDENTIALS);

    restRouting.applyAuthentication(all);

    all.put(IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED, "FALSE");
    return all;
  }

  /**
   * The IRC endpoint and authentication for one catalog, resolved by the caller from
   * deployment-level Spark configuration.
   *
   * <p>Authentication is deliberately restricted to the two forms that reach the IRC without a
   * token-endpoint round trip. iceberg-core's {@code OAuth2Manager} dispatches on the credential
   * key: a value under {@code token} is sent straight through as {@code Authorization: Bearer},
   * whereas {@code credential} triggers a client_credentials fetch and the five OAuth2 token-type
   * URNs trigger an RFC 8693 exchange. Both of those post to {@code <uri>/v1/oauth/tokens}, which
   * the Gravitino IRC does not serve, so they fail as a 404 rather than as an auth error. Never
   * emit {@code credential} or a token-type URN.
   */
  public static final class IcebergRestRouting {

    private final String restUri;
    private final String catalogName;
    @Nullable private final String bearerToken;
    @Nullable private final String authorizationHeader;

    private IcebergRestRouting(
        String restUri,
        String catalogName,
        @Nullable String bearerToken,
        @Nullable String authorizationHeader) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(restUri), "Iceberg REST URI should not be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(catalogName), "Gravitino catalog name should not be empty");
      this.restUri = restUri;
      this.catalogName = catalogName;
      this.bearerToken = bearerToken;
      this.authorizationHeader = authorizationHeader;
    }

    /**
     * Routes to the IRC without presenting credentials, for an IRC that does not require
     * authentication.
     *
     * @param restUri the IRC URI.
     * @param catalogName the Gravitino catalog name, used as the REST warehouse.
     * @return the routing configuration.
     */
    public static IcebergRestRouting anonymous(String restUri, String catalogName) {
      return new IcebergRestRouting(restUri, catalogName, null, null);
    }

    /**
     * Routes to the IRC presenting {@code bearerToken} under Iceberg's direct-bearer {@code token}
     * property, which is sent as {@code Authorization: Bearer} with no token-endpoint round trip.
     *
     * @param restUri the IRC URI.
     * @param catalogName the Gravitino catalog name, used as the REST warehouse.
     * @param bearerToken the bearer token the connector presents to Gravitino.
     * @return the routing configuration.
     */
    public static IcebergRestRouting withBearerToken(
        String restUri, String catalogName, String bearerToken) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(bearerToken), "Bearer token should not be empty");
      return new IcebergRestRouting(restUri, catalogName, bearerToken, null);
    }

    /**
     * Routes to the IRC presenting {@code authorizationHeader} verbatim through Iceberg's {@code
     * header.} passthrough. Used for Gravitino's {@code simple} and {@code basic} auth, both of
     * which are a {@code Basic} credential rather than a bearer.
     *
     * @param restUri the IRC URI.
     * @param catalogName the Gravitino catalog name, used as the REST warehouse.
     * @param authorizationHeader the full Authorization header value, e.g. {@code Basic dXNlcjpw}.
     * @return the routing configuration.
     */
    public static IcebergRestRouting withAuthorizationHeader(
        String restUri, String catalogName, String authorizationHeader) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(authorizationHeader), "Authorization header should not be empty");
      return new IcebergRestRouting(restUri, catalogName, null, authorizationHeader);
    }

    private String restUri() {
      return restUri;
    }

    private String catalogName() {
      return catalogName;
    }

    private void applyAuthentication(Map<String, String> icebergProperties) {
      if (bearerToken != null) {
        icebergProperties.put(ICEBERG_REST_TOKEN, bearerToken);
      } else if (authorizationHeader != null) {
        icebergProperties.put(ICEBERG_REST_AUTHORIZATION_HEADER, authorizationHeader);
      }
    }
  }
}
