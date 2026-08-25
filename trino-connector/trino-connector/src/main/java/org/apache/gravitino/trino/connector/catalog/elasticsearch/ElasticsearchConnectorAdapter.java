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
package org.apache.gravitino.trino.connector.catalog.elasticsearch;

import io.trino.spi.TrinoException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;

/**
 * Transforms a Gravitino Elasticsearch catalog into Trino's native elasticsearch connector. The
 * Gravitino catalog carries elasticsearch.hosts (comma-separated, scheme-qualified); Trino's
 * connector wants a single host + port, so the first host is used.
 */
public class ElasticsearchConnectorAdapter implements CatalogConnectorAdapter {

  private static final String CONNECTOR_ELASTICSEARCH = "elasticsearch";
  private static final String BYPASS_PREFIX = "trino.bypass.";
  private static final int DEFAULT_ES_PORT = 9200;

  @Override
  public Map<String, String> buildInternalConnectorConfig(
      GravitinoCatalog catalog, Credential[] credentials) throws Exception {
    Map<String, String> config = new HashMap<>();
    Map<String, String> props = catalog.getProperties();

    String hosts = props.get("elasticsearch.hosts");
    if (hosts == null || hosts.isBlank()) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          "elasticsearch.hosts is required for an elasticsearch catalog");
    }

    String first = hosts.split(",")[0].trim();
    URI uri = URI.create(first.contains("://") ? first : "http://" + first);
    String host = uri.getHost();
    int port = uri.getPort() == -1 ? DEFAULT_ES_PORT : uri.getPort();
    boolean tls = "https".equalsIgnoreCase(uri.getScheme());

    config.put("elasticsearch.host", host);
    config.put("elasticsearch.port", String.valueOf(port));
    config.put("elasticsearch.default-schema-name", "default");

    if (tls) {
      config.put("elasticsearch.tls.enabled", "true");
      // Trino's ES connector has no full skip-verify; verify-hostnames only drops
      // the hostname check. Certificate trust for an internal-CA cluster still
      // requires a truststore supplied via trino.bypass.elasticsearch.tls.truststore-path.
      if ("true".equalsIgnoreCase(props.getOrDefault("elasticsearch.tls.skip-verify", "false"))) {
        config.put("elasticsearch.tls.verify-hostnames", "false");
      }
    }

    String user = props.get("elasticsearch.username");
    if (user != null && !user.isBlank()) {
      config.put("elasticsearch.security", "PASSWORD");
      config.put("elasticsearch.auth.user", user);
      String password = props.get("elasticsearch.password");
      if (password != null) {
        config.put("elasticsearch.auth.password", password);
      }
    }

    for (Map.Entry<String, String> e : props.entrySet()) {
      if (e.getKey().startsWith(BYPASS_PREFIX)) {
        config.put(e.getKey().substring(BYPASS_PREFIX.length()), e.getValue());
      }
    }
    return config;
  }

  @Override
  public String internalConnectorName() {
    return CONNECTOR_ELASTICSEARCH;
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    return new CatalogConnectorMetadataAdapter(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        new GeneralDataTypeTransformer());
  }
}
