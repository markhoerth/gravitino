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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.gravitino.spark.connector.iceberg.IcebergPropertiesConverter.IcebergRestRouting;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.procedures.SparkProcedures;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.iceberg.catalog.Procedure;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The GravitinoIcebergCatalog class extends the BaseCatalog to integrate with the Apache Iceberg
 * table format, providing specialized support for Iceberg-specific functionalities within Apache
 * Spark's ecosystem. This implementation can further adapt to specific interfaces such as
 * StagingTableCatalog and FunctionCatalog, allowing for advanced operations like table staging and
 * function management tailored to the needs of Iceberg tables.
 */
public class GravitinoIcebergCatalog extends BaseCatalog
    implements FunctionCatalog, ProcedureCatalog, HasIcebergCatalog {

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    Optional<IcebergRestRouting> restRouting = resolveRestRouting(name);
    String jdbcDriver = properties.get(IcebergConstants.GRAVITINO_JDBC_DRIVER);
    if (!restRouting.isPresent() && StringUtils.isNotBlank(jdbcDriver)) {
      // If `spark.sql.hive.metastore.jars` is set, Spark will use an isolated client class loader
      // to load JDBC drivers, which makes Iceberg could not find corresponding JDBC driver.
      // Skipped when routing through the IRC: Iceberg opens no JDBC connection then, and the
      // driver need not be on the Spark classpath at all.
      try {
        Class.forName(jdbcDriver);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    String catalogBackendName = IcebergPropertiesUtils.getCatalogBackendName(properties);
    Map<String, String> all = buildSparkCatalogProperties(options, properties, restRouting);
    TableCatalog icebergCatalog = new SparkCatalog();
    icebergCatalog.initialize(catalogBackendName, new CaseInsensitiveStringMap(all));
    return icebergCatalog;
  }

  /**
   * Builds the properties handed to Iceberg's {@code SparkCatalog}.
   *
   * <p>When routing through the IRC no catalog-level credential is fetched: the IRC vends per table
   * on the loadTable response, scoped to that table's location, and stamping a catalog-level
   * credential in alongside would leave Iceberg holding two sets of keys with no defined
   * precedence.
   */
  @VisibleForTesting
  Map<String, String> buildSparkCatalogProperties(
      CaseInsensitiveStringMap options,
      Map<String, String> properties,
      Optional<IcebergRestRouting> restRouting) {
    if (restRouting.isPresent()) {
      return IcebergPropertiesConverter.restRoutingInstance(restRouting.get())
          .toSparkCatalogProperties(options, properties);
    }
    Map<String, String> all =
        getPropertiesConverter().toSparkCatalogProperties(options, properties);
    CredentialPropertyUtils.applyIcebergCredentials(
        CredentialPropertyUtils.getCredentials(gravitinoCatalogClient), all);
    return all;
  }

  /**
   * Resolves deployment-level IRC routing for {@code catalogName}, empty when {@link
   * GravitinoSparkConfig#GRAVITINO_ICEBERG_REST_URI} is unset.
   */
  private Optional<IcebergRestRouting> resolveRestRouting(String catalogName) {
    SparkConf sparkConf = SparkSession.active().sparkContext().getConf();
    String restUri = sparkConf.get(GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_URI, null);
    if (StringUtils.isBlank(restUri)) {
      return Optional.empty();
    }

    String authType =
        sparkConf.get(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.SIMPLE_AUTH_TYPE);
    if (AuthProperties.isSimple(authType)) {
      // Gravitino's simple auth is a Basic credential over a synthetic password; see
      // SimpleTokenProvider. Reproduce the header the client would send rather than re-deriving it.
      String sparkUser = SparkSession.active().sparkContext().sparkUser();
      return Optional.of(
          IcebergRestRouting.withAuthorizationHeader(
              restUri, catalogName, basicAuthorizationHeader(sparkUser, "dummy")));
    } else if (AuthProperties.isBasic(authType)) {
      String username = getRequiredConf(sparkConf, GravitinoSparkConfig.GRAVITINO_BASIC_USERNAME);
      String password = getRequiredConf(sparkConf, GravitinoSparkConfig.GRAVITINO_BASIC_PASSWORD);
      return Optional.of(
          IcebergRestRouting.withAuthorizationHeader(
              restUri, catalogName, basicAuthorizationHeader(username, password)));
    }

    // oauth2 and kerberos mint a credential per request through a token provider, which cannot be
    // expressed as a static catalog property. Fail loudly rather than routing unauthenticated.
    throw new UnsupportedOperationException(
        String.format(
            "%s does not support auth type '%s' yet; only '%s' and '%s' can be expressed as "
                + "Iceberg REST catalog properties",
            GravitinoSparkConfig.GRAVITINO_ICEBERG_REST_URI,
            authType,
            AuthProperties.SIMPLE_AUTH_TYPE,
            AuthProperties.BASIC_AUTH_TYPE));
  }

  private static String basicAuthorizationHeader(String username, String password) {
    String userInformation = username + ":" + password;
    return "Basic "
        + Base64.getEncoder().encodeToString(userInformation.getBytes(StandardCharsets.UTF_8));
  }

  private static String getRequiredConf(SparkConf sparkConf, String key) {
    String value = sparkConf.get(key, null);
    if (StringUtils.isBlank(value)) {
      throw new IllegalArgumentException(key + " should not be empty");
    }
    return value;
  }

  @Override
  protected org.apache.spark.sql.connector.catalog.Table createSparkTable(
      Identifier identifier,
      Table gravitinoTable,
      org.apache.spark.sql.connector.catalog.Table sparkTable,
      TableCatalog sparkIcebergCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    return new SparkIcebergTable(
        identifier,
        gravitinoTable,
        (SparkTable) sparkTable,
        (SparkCatalog) sparkIcebergCatalog,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return IcebergPropertiesConverter.getInstance();
  }

  @Override
  protected SparkTransformConverter getSparkTransformConverter() {
    return new SparkTransformConverter(true);
  }

  @Override
  public Identifier[] listFunctions(String[] namespace) throws NoSuchNamespaceException {
    return isIcebergFunctionNamespace(namespace)
        ? ((SparkCatalog) sparkCatalog).listFunctions(namespace)
        : super.listFunctions(namespace);
  }

  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    return isIcebergFunctionNamespace(ident.namespace())
        ? ((SparkCatalog) sparkCatalog).loadFunction(ident)
        : super.loadFunction(ident);
  }

  /**
   * Procedures will validate the equality of the catalog registered to Spark catalogManager and the
   * catalog passed to `ProcedureBuilder` which invokes loadProcedure(). To meet the requirement ,
   * override the method to pass `GravitinoIcebergCatalog` to the `ProcedureBuilder` instead of the
   * internal spark catalog.
   */
  @Override
  public Procedure loadProcedure(Identifier identifier) throws NoSuchProcedureException {
    String[] namespace = identifier.namespace();
    String name = identifier.name();

    try {
      if (isSystemNamespace(namespace)) {
        SparkProcedures.ProcedureBuilder builder = SparkProcedures.newBuilder(name);
        if (builder != null) {
          return builder.withTableCatalog(this).build();
        }
      }
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | ClassNotFoundException e) {
      throw new RuntimeException("Failed to load Iceberg Procedure " + identifier, e);
    }

    throw new NoSuchProcedureException(identifier);
  }

  @Override
  public Catalog icebergCatalog() {
    return ((SparkCatalog) sparkCatalog).icebergCatalog();
  }

  @Override
  public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier ident, String version)
      throws NoSuchTableException {
    try {
      org.apache.gravitino.rel.Table gravitinoTable = loadGravitinoTable(ident);
      org.apache.spark.sql.connector.catalog.Table sparkTable = loadSparkTable(ident, version);
      // Will create a catalog specific table
      return createSparkTable(
          ident,
          gravitinoTable,
          sparkTable,
          sparkCatalog,
          propertiesConverter,
          sparkTransformConverter,
          getSparkTypeConverter());
    } catch (org.apache.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public org.apache.spark.sql.connector.catalog.Table loadTable(Identifier ident, long timestamp)
      throws NoSuchTableException {
    try {
      org.apache.gravitino.rel.Table gravitinoTable = loadGravitinoTable(ident);
      org.apache.spark.sql.connector.catalog.Table sparkTable = loadSparkTable(ident, timestamp);
      // Will create a catalog specific table
      return createSparkTable(
          ident,
          gravitinoTable,
          sparkTable,
          sparkCatalog,
          propertiesConverter,
          sparkTransformConverter,
          getSparkTypeConverter());
    } catch (org.apache.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  private boolean isIcebergFunctionNamespace(String[] namespace) {
    try {
      return namespace.length == 0 || isSystemNamespace(namespace);
    } catch (IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException
        | ClassNotFoundException e) {
      throw new RuntimeException("Failed to check Iceberg function namespace", e);
    }
  }

  private boolean isSystemNamespace(String[] namespace)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
          ClassNotFoundException {
    Class<?> baseCatalog = Class.forName("org.apache.iceberg.spark.BaseCatalog");
    Method isSystemNamespace = baseCatalog.getDeclaredMethod("isSystemNamespace", String[].class);
    isSystemNamespace.setAccessible(true);
    return (Boolean) isSystemNamespace.invoke(baseCatalog, (Object) namespace);
  }

  private org.apache.spark.sql.connector.catalog.Table loadSparkTable(
      Identifier ident, String version) {
    try {
      return sparkCatalog.loadTable(ident, version);
    } catch (NoSuchTableException e) {
      throw new RuntimeException(
          String.format(
              "Failed to load the real sparkTable: %s",
              String.join(".", getDatabase(ident), ident.name())),
          e);
    }
  }

  private org.apache.spark.sql.connector.catalog.Table loadSparkTable(
      Identifier ident, long timestamp) {
    try {
      return sparkCatalog.loadTable(ident, timestamp);
    } catch (NoSuchTableException e) {
      throw new RuntimeException(
          String.format(
              "Failed to load the real sparkTable: %s",
              String.join(".", getDatabase(ident), ident.name())),
          e);
    }
  }
}
