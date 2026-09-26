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
# Packages the Elasticsearch catalog connector and the Elasticsearch authorization plugin as a kit:
# installable jars, the source of both modules, and the documentation.
#
#   ./package-es-kit.sh
#   TARGET_VERSION=1.3.0 ./package-es-kit.sh
#
# TARGET_VERSION must match the Gravitino version running on the destination server.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PACKAGE_DIR="$REPO_ROOT/distribution/package"
BUILD_VERSION="$(grep -E '^version *=' "$REPO_ROOT/gradle.properties" | head -1 | sed 's/.*= *//' | tr -d '[:space:]')"
TARGET_VERSION="${TARGET_VERSION:-$BUILD_VERSION}"
KIT="gravitino-elasticsearch-kit-$TARGET_VERSION"
STAGE="$REPO_ROOT/build/$KIT"
ARCHIVE="$REPO_ROOT/build/$KIT.tar.gz"

CATALOG_MODULE="$REPO_ROOT/catalogs/catalog-elasticsearch"
AUTHZ_MODULE="$REPO_ROOT/authorizations/authorization-elasticsearch"

echo "Building at version $BUILD_VERSION, packaging for $TARGET_VERSION"
if [[ "$BUILD_VERSION" != "$TARGET_VERSION" ]]; then
  echo "The build version and the target version differ." >&2
  echo "Set 'version = $TARGET_VERSION' in gradle.properties and rebuild." >&2
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
mkdir -p "$STAGE/install/catalogs" "$STAGE/install/authorizations" "$STAGE/source"
cp -r "$PACKAGE_DIR/catalogs/elasticsearch" "$STAGE/install/catalogs/"
cp -r "$PACKAGE_DIR/authorizations/elasticsearch" "$STAGE/install/authorizations/"

# Source of both modules, so the kit can be read, modified and rebuilt.
mkdir -p "$STAGE/source/catalogs/catalog-elasticsearch" \
         "$STAGE/source/authorizations/authorization-elasticsearch"
cp -r "$CATALOG_MODULE/src" "$STAGE/source/catalogs/catalog-elasticsearch/"
cp "$CATALOG_MODULE/build.gradle.kts" "$STAGE/source/catalogs/catalog-elasticsearch/"
[[ -f "$CATALOG_MODULE/README.md" ]] && \
  cp "$CATALOG_MODULE/README.md" "$STAGE/source/catalogs/catalog-elasticsearch/"
cp -r "$AUTHZ_MODULE/src" "$STAGE/source/authorizations/authorization-elasticsearch/"
cp "$AUTHZ_MODULE/build.gradle.kts" "$STAGE/source/authorizations/authorization-elasticsearch/"
cp "$AUTHZ_MODULE/README.md" "$STAGE/source/authorizations/authorization-elasticsearch/"
mkdir -p "$STAGE/source/authorizations/authorization-elasticsearch/dev"
cp "$AUTHZ_MODULE/dev/es-authz-smoke.sh" \
   "$STAGE/source/authorizations/authorization-elasticsearch/dev/"

cat > "$STAGE/OVERVIEW.md" <<EOF
# Governing Elasticsearch with Gravitino

Two components for Gravitino $TARGET_VERSION. Both are prototypes provided for evaluation, not a
supported product.

| Component | Installs as | What it does |
| --- | --- | --- |
| Elasticsearch catalog connector | \`catalogs/elasticsearch\` | Federates a cluster: each index becomes a table, each mapping becomes columns |
| Elasticsearch authorization plugin | \`authorizations/elasticsearch\` | Writes Gravitino roles and grants into the cluster's own security configuration |

Both are needed to govern Elasticsearch. The connector supplies the tables that privileges are
granted on. The plugin puts those privileges into the cluster.

## The model

Gravitino is the policy authority. Elasticsearch is the enforcement point. No query engine sits in
the request path: applications connect straight to the cluster, and Elasticsearch makes every
access decision itself, using roles that Gravitino wrote and keeps current.

Nothing in the read path goes through Gravitino, so the cluster continues to enforce these
privileges whether or not Gravitino is running. Gravitino's involvement is at grant time and on the
reconciliation interval.

## What is pushed down

| Gravitino | Elasticsearch |
| --- | --- |
| Role with privileges | Role named \`gravitino_<metalake>_<role>\` |
| Role granted to a user | Role mapping rule on \`username\`, and the role on a native user record if one exists |
| Role granted to a group | Role mapping rule on the \`groups\` claim |
| Catalog or schema grant | Index pattern, scoped by \`index.prefix\` |
| Table grant | Exact index name |
| \`SELECT_TABLE\` | \`read\`, \`view_index_metadata\` |
| \`MODIFY_TABLE\` | \`write\`, \`view_index_metadata\` |
| \`CREATE_TABLE\` | \`create_index\` |
| \`USE_CATALOG\`, \`USE_SCHEMA\` | \`view_index_metadata\` |
| Row filter | Document-level security query |
| Column restriction | Field-level security, the field removed |

Privileges with no Elasticsearch counterpart are dropped rather than approximated.

Roles the plugin owns carry \`"managed_by": "gravitino"\` in their metadata, along with the metalake
and the original role name.

## Deny

Elasticsearch roles only add privileges. No entry in a role subtracts from another, so a deny cannot
be written directly. Gravitino realises it by computing the indices the role still reaches and
writing those in place of the pattern.

A catalog grant of \`SELECT_TABLE\` with a deny on one index therefore produces explicit entries for
the indices that remain, and the denied index appears nowhere in the role.

One consequence: an index created after that point is not in the list, so it stays outside the role
until the next reconciliation pass, which runs on
\`authorization.elasticsearch.reconcile.interval-seconds\`.

A second consequence, and a rule for role design: roles union. A user holding a second role that
grants the denied index gets access to it, because there is no deny in Elasticsearch to act as a
tie-break. A deny governs only the role it is written on.

## Identity

Elasticsearch stores no users in this model, and the plugin never creates any. The identity that
matters is the one the caller authenticates as, and it has to be the same identity Gravitino
granted on. Two ways that happens:

**External realm (JWT, OIDC, SAML).** The caller presents a token, the realm produces a username
and a groups claim, and the role mapping the plugin wrote is evaluated against them. The username
in the token must match the username in Gravitino, and group names must match too. Both sides
normally derive from the same identity provider, so the question is which claim each one uses.

**Native realm (local Elasticsearch accounts).** Role mappings are not consulted for native users,
because their roles live on the user record. So on grant the plugin also adds the role to a native
user of the same name when one exists, and removes it on revoke. Passwords and other roles are left
untouched.

If no native user of that name exists, the role is still written. An administrator can create the
account and assign \`gravitino_<metalake>_<role>\` to it, and from then on grants and revokes in
Gravitino reach that user through the role itself.

Set \`authorization.elasticsearch.native-user.bind\` to \`false\` to skip the native-user handling
on clusters that use external realms only.

### Two things to plan for

**Names must agree.** If Gravitino grants to \`mark\` and the token asserts
\`mark.hoerth@example.com\`, the rule matches nobody. Nothing errors: the grant succeeds, the role
and mapping are valid, and the user simply has no access.

**Applications must authenticate as the end user.** If they connect with a shared service account,
Elasticsearch sees one identity and every user of that application gets that account's privileges.
Per-user identity is what makes any of this enforce anything.

**Hand-assigned roles are invisible to Gravitino.** Assigning a \`gravitino_*\` role to a user
directly grants that user the role's privileges, and Gravitino has no record of it. Reconciliation
rewrites what a role contains, never who holds it.

## Licensing

Index privileges work on any Elastic subscription level. Document-level and field-level security,
and the external realms, require a paid tier. The plugin logs the cluster's license level when the
catalog loads, and warns when it reports \`basic\`.

## What is not included

- Value masking on read. Field-level security removes a field rather than transforming it, so
  partial reveal or hashing needs a masked copy written at ingest and granted in place of the raw
  field.
- Row filters using functions, subqueries or joins. The translatable subset is comparisons, IN,
  AND/OR, ranges and null checks.
- Writes to indices under row or column restrictions, which Elasticsearch does not permit.
- A trust store option. TLS verification is either on or skipped
  (\`elasticsearch.tls.skip-verify\`, \`authorization.elasticsearch.tls.insecure\`), so a private
  certificate authority needs the certificate in the JVM trust store of the Gravitino server.
EOF

cat > "$STAGE/INSTALL.md" <<EOF
# Installing

Two directories drop into an existing Gravitino $TARGET_VERSION installation. The server version
must be $TARGET_VERSION: the bundle carries Gravitino api and core jars, and a mismatch fails when
the catalog loads.

\`\`\`
tar xzf $KIT.tar.gz
cd $KIT
cp -r install/catalogs/elasticsearch       \\\$GRAVITINO_HOME/catalogs/
cp -r install/authorizations/elasticsearch \\\$GRAVITINO_HOME/authorizations/
\`\`\`

## Enable authorization

Privilege pushdown requires authorization to be switched on in \`conf/gravitino.conf\`:

\`\`\`
gravitino.authorization.enable = true
gravitino.authorization.serviceAdmins = <the administrator account>
\`\`\`

Without it the role and permission endpoints return an error and nothing reaches Elasticsearch.
Restart the server after the change.

## Create a governed catalog

One catalog carries the connector properties and the authorization properties together:

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
\`source/authorizations/authorization-elasticsearch/README.md\`.

\`index.prefix\` scopes catalog-level grants. With a prefix they become the pattern
\`<prefix>*\`; with an empty prefix a catalog grant covers every index on the cluster.

## Confirm the plugin loaded

The server log records the cluster's license level as the catalog loads:

\`\`\`
grep "Elasticsearch cluster reports license type" \\\$GRAVITINO_HOME/logs/gravitino-server.log
\`\`\`

No such line means the plugin did not load, and grants will not reach Elasticsearch.

## Rebuilding from source

\`source/\` holds both modules. Copy them into a Gravitino $TARGET_VERSION source tree at the same
paths, add the authorization module to \`settings.gradle.kts\`, and build:

\`\`\`
./gradlew :catalogs:catalog-elasticsearch:build
./gradlew :authorizations:authorization-elasticsearch:build
./gradlew compileDistribution -x test
\`\`\`
EOF

cat > "$STAGE/VERIFY.md" <<EOF
# Verifying the installation

A privilege set in Gravitino, observed in Elasticsearch, then enforced against a real query. The
example uses a native Elasticsearch user, which needs no identity provider, and takes a few minutes.

Substitute your own cluster URL, credentials, metalake name and index names throughout. The example
assumes a catalog named \`search\` created as in INSTALL.md, with \`index.prefix\` set to \`demo_\`,
and two indices \`demo_claims\` and \`demo_pii\`.

## 1. Confirm the catalog sees the indices

\`\`\`
curl -s -H 'Accept: application/vnd.gravitino.v1+json' \\
  https://gravitino.example.com:8090/api/metalakes/demo/catalogs/search/schemas/default/tables
\`\`\`

Each index appears as a table under the synthetic schema \`default\`.

## 2. Create a role with a read privilege on the catalog

\`\`\`
curl -s -X POST -H 'Content-Type: application/json' \\
  -H 'Accept: application/vnd.gravitino.v1+json' \\
  -d '{
        "name": "ANALYST",
        "properties": {},
        "securableObjects": [
          {"fullName": "search", "type": "CATALOG",
           "privileges": [{"name": "SELECT_TABLE", "condition": "ALLOW"}]}
        ]
      }' \\
  https://gravitino.example.com:8090/api/metalakes/demo/roles
\`\`\`

## 3. Read the role back from Elasticsearch

\`\`\`
curl -s -u <account>:<password> \\
  https://es.example.com:9200/_security/role/gravitino_demo_analyst
\`\`\`

The catalog grant became the index pattern \`demo_*\` with the privileges \`read\` and
\`view_index_metadata\`. The role name is lower-cased and carries the metalake.

## 4. Deny one index, and see the pattern resolve

\`\`\`
curl -s -X PUT -H 'Content-Type: application/json' \\
  -H 'Accept: application/vnd.gravitino.v1+json' \\
  -d '{"privileges":[{"name":"SELECT_TABLE","condition":"DENY"}]}' \\
  https://gravitino.example.com:8090/api/metalakes/demo/permissions/roles/ANALYST/table/search.default.demo_pii/grant

curl -s -u <account>:<password> \\
  https://es.example.com:9200/_security/role/gravitino_demo_analyst
\`\`\`

The pattern is gone, replaced by explicit entries for the indices that remain allowed.
\`demo_pii\` appears nowhere in the role.

## 5. Create a local Elasticsearch user

\`\`\`
curl -s -u <account>:<password> -X POST \\
  -H 'Content-Type: application/json' \\
  -d '{"password":"<a password>","roles":["kibana_user"]}' \\
  https://es.example.com:9200/_security/user/mark
\`\`\`

The user carries one unrelated role, which the next step must leave intact.

## 6. Grant the role to that user in Gravitino

\`\`\`
curl -s -X POST -H 'Content-Type: application/json' \\
  -H 'Accept: application/vnd.gravitino.v1+json' \\
  -d '{"name":"mark"}' \\
  https://gravitino.example.com:8090/api/metalakes/demo/users

curl -s -X PUT -H 'Content-Type: application/json' \\
  -H 'Accept: application/vnd.gravitino.v1+json' \\
  -d '{"roleNames":["ANALYST"]}' \\
  https://gravitino.example.com:8090/api/metalakes/demo/permissions/users/mark/grant
\`\`\`

## 7. Confirm the binding

\`\`\`
curl -s -u <account>:<password> https://es.example.com:9200/_security/user/mark
\`\`\`

The user now holds \`kibana_user\` and \`gravitino_demo_analyst\`. No password appears in the
record, because the update left it alone:

\`\`\`
curl -s -u mark:<a password> https://es.example.com:9200/_security/_authenticate
\`\`\`

## 8. Query as that user

\`\`\`
curl -s -u mark:<a password> "https://es.example.com:9200/demo_claims/_search?size=1"
curl -s -u mark:<a password> "https://es.example.com:9200/demo_pii/_search?size=1"
\`\`\`

The first returns a hit. The second is refused with a 403 naming the effective roles:

\`\`\`
action [indices:data/read/search] is unauthorized for user [mark] with effective roles
[gravitino_demo_analyst,kibana_user] on indices [demo_pii]
\`\`\`

Elasticsearch refused the query itself, using a privilege that was set in Gravitino and never
touched in Elasticsearch by hand.

## 9. Clean up

\`\`\`
curl -s -u <account>:<password> -X DELETE https://es.example.com:9200/_security/user/mark
curl -s -X DELETE -H 'Accept: application/vnd.gravitino.v1+json' \\
  https://gravitino.example.com:8090/api/metalakes/demo/roles/ANALYST
\`\`\`

Deleting the role in Gravitino removes the Elasticsearch role and its mapping.

## Checking Elasticsearch behaviour on its own

\`source/authorizations/authorization-elasticsearch/dev/es-authz-smoke.sh\` writes the same role
shapes directly to a cluster, with no Gravitino involved, and reports what Elasticsearch does with
them: index privileges, field restrictions, row filters, and the effect of a second unrestricted
role. It reports the license tier, skips the paid-tier checks on basic, and cleans up after itself.

\`\`\`
ES_URL=https://es.example.com:9200 ES_USER=<account> ES_PASSWORD=<password> ./es-authz-smoke.sh
\`\`\`
EOF

cat > "$STAGE/README.md" <<EOF
# Elasticsearch connector and privilege pushdown for Gravitino $TARGET_VERSION

- \`OVERVIEW.md\` explains what the two components do, what is pushed down, how deny is handled,
  and how identity has to line up.
- \`INSTALL.md\` covers installation, the required configuration, and creating a governed catalog.
- \`VERIFY.md\` walks through setting a privilege in Gravitino and watching Elasticsearch enforce it.
- \`install/\` holds the jars for both components.
- \`source/\` holds the source of both modules, with the plugin's own README and the smoke script.

Prototypes provided for evaluation. Not a supported product.
EOF

echo "== Archiving =="
rm -f "$ARCHIVE"
(cd "$(dirname "$STAGE")" && tar czf "$ARCHIVE" "$KIT")
(cd "$(dirname "$ARCHIVE")" && sha256sum "$(basename "$ARCHIVE")" > "$(basename "$ARCHIVE").sha256")

echo
echo "Wrote $ARCHIVE"
du -h "$ARCHIVE"
tar tzf "$ARCHIVE" | grep -vE "libs/|/src/" | head -30
