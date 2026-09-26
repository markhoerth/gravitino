#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Packages the Elasticsearch catalog connector and the Elasticsearch authorization plugin into a
# single archive that drops into an existing Gravitino deployment.
#
#   ./package-es-plugins.sh            package at the version in gradle.properties
#   TARGET_VERSION=1.3.0 ./package-es-plugins.sh
#
# TARGET_VERSION must match the Gravitino version running on the destination server. The jars
# carry Gravitino api and core alongside the connector, so a mismatch fails at catalog load.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PACKAGE_DIR="$REPO_ROOT/distribution/package"
BUILD_VERSION="$(grep -E '^version *=' "$REPO_ROOT/gradle.properties" | head -1 | sed 's/.*= *//' | tr -d '[:space:]')"
TARGET_VERSION="${TARGET_VERSION:-$BUILD_VERSION}"
STAGE="$REPO_ROOT/build/es-plugins-$TARGET_VERSION"
ARCHIVE="$REPO_ROOT/build/gravitino-elasticsearch-plugins-$TARGET_VERSION.tar.gz"

echo "Building at version $BUILD_VERSION, packaging for $TARGET_VERSION"
if [[ "$BUILD_VERSION" != "$TARGET_VERSION" ]]; then
  echo "The build version and the target version differ." >&2
  echo "Set 'version = $TARGET_VERSION' in gradle.properties and rebuild, or the jars will not" >&2
  echo "match the destination server." >&2
  exit 1
fi

echo "== Building the distribution =="
(cd "$REPO_ROOT" && ./gradlew compileDistribution -x test)

CATALOG_LIBS="$PACKAGE_DIR/catalogs/elasticsearch/libs"
AUTHZ_LIBS="$PACKAGE_DIR/authorizations/elasticsearch/libs"
for dir in "$CATALOG_LIBS" "$AUTHZ_LIBS"; do
  if [[ ! -d "$dir" ]] || [[ -z "$(ls -A "$dir" 2>/dev/null)" ]]; then
    echo "Expected staged jars at $dir but found none." >&2
    exit 1
  fi
done

echo "== Staging =="
rm -rf "$STAGE"
mkdir -p "$STAGE/catalogs" "$STAGE/authorizations"
cp -r "$PACKAGE_DIR/catalogs/elasticsearch" "$STAGE/catalogs/"
cp -r "$PACKAGE_DIR/authorizations/elasticsearch" "$STAGE/authorizations/"

cp "$REPO_ROOT/authorizations/authorization-elasticsearch/README.md" \
  "$STAGE/authorizations/elasticsearch/README.md"
if [[ -f "$REPO_ROOT/catalogs/catalog-elasticsearch/README.md" ]]; then
  cp "$REPO_ROOT/catalogs/catalog-elasticsearch/README.md" \
    "$STAGE/catalogs/elasticsearch/README.md"
fi

cat > "$STAGE/INSTALL.md" <<EOF
# Elasticsearch catalog connector and authorization plugin

Two components for Gravitino $TARGET_VERSION:

- \`catalogs/elasticsearch\` federates an Elasticsearch cluster, presenting each index as a table
  under a synthetic \`default\` schema.
- \`authorizations/elasticsearch\` pushes Gravitino roles and grants into the Elasticsearch
  security API, so clients querying Elasticsearch directly are subject to Gravitino privileges.

Both are needed to govern Elasticsearch. The connector supplies the tables that privileges are
granted on; the plugin writes those privileges into the cluster.

These are prototypes provided for evaluation. They are not a supported product.

## Install

Stop the server, unpack into the Gravitino installation directory, and restart:

\`\`\`
tar xzf gravitino-elasticsearch-plugins-$TARGET_VERSION.tar.gz
cp -r catalogs/elasticsearch   \$GRAVITINO_HOME/catalogs/
cp -r authorizations/elasticsearch \$GRAVITINO_HOME/authorizations/
\`\`\`

The server version must be $TARGET_VERSION. The bundle carries Gravitino api and core jars, and a
version mismatch fails when the catalog loads.

## Enable authorization

Privilege pushdown requires authorization to be switched on in \`conf/gravitino.conf\`:

\`\`\`
gravitino.authorization.enable = true
gravitino.authorization.serviceAdmins = <the administrator account>
\`\`\`

Without it the role and permission endpoints return an error and nothing reaches Elasticsearch.

## Create a governed catalog

One catalog carries both the connector properties and the authorization properties:

\`\`\`
POST /api/metalakes/<metalake>/catalogs
{
  "name": "search",
  "type": "RELATIONAL",
  "provider": "elasticsearch",
  "properties": {
    "elasticsearch.hosts": "https://es.example.com:9200",
    "elasticsearch.username": "<account>",
    "elasticsearch.password": "<password>",

    "authorization-provider": "elasticsearch",
    "authorization.elasticsearch.url": "https://es.example.com:9200",
    "authorization.elasticsearch.auth.type": "basic",
    "authorization.elasticsearch.username": "<account>",
    "authorization.elasticsearch.password": "<password>",
    "authorization.elasticsearch.index.prefix": "",
    "authorization.elasticsearch.reconcile.interval-seconds": "300"
  }
}
\`\`\`

The account the plugin authenticates as needs \`manage_security\`. The full property list is in
\`authorizations/elasticsearch/README.md\`.

\`index.prefix\` scopes catalog-level grants: with a prefix they become the pattern \`<prefix>*\`,
and with an empty prefix a catalog grant covers every index on the cluster.

## Verify

After creating the catalog, the server log records the cluster's license level:

\`\`\`
grep "Elasticsearch cluster reports license type" \$GRAVITINO_HOME/logs/gravitino-server.log
\`\`\`

A \`basic\` license enforces index privileges but not row filters or field restrictions, which
require a paid tier.

Then create a role holding a privilege on the catalog and read it back from Elasticsearch:

\`\`\`
curl -u <account>:<password> https://es.example.com:9200/_security/role/gravitino_*
\`\`\`

Roles the plugin owns carry \`"managed_by": "gravitino"\` in their metadata and are reverted if
edited by hand, on the reconciliation interval.
EOF

echo "== Archiving =="
rm -f "$ARCHIVE"
(cd "$(dirname "$STAGE")" && tar czf "$ARCHIVE" "$(basename "$STAGE")")
(cd "$(dirname "$ARCHIVE")" && sha256sum "$(basename "$ARCHIVE")" > "$(basename "$ARCHIVE").sha256")

echo
echo "Wrote $ARCHIVE"
tar tzf "$ARCHIVE" | head -20
