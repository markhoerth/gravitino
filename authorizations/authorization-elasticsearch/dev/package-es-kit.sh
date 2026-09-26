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

cat > "$STAGE/OVERVIEW.md" <<'OVERVIEW_EOF'
# Governing Elasticsearch with Gravitino

Two components for Gravitino @VERSION@. Both are prototypes provided for evaluation, not a
supported product.

| Component | Installs as | What it does |
| --- | --- | --- |
| Elasticsearch catalog connector | `catalogs/elasticsearch` | Federates a cluster: each index becomes a table, each mapping becomes columns |
| Elasticsearch authorization plugin | `authorizations/elasticsearch` | Writes Gravitino roles and grants into the cluster's own security configuration |

Both are needed to govern Elasticsearch. The connector supplies the tables that privileges are
granted on. The plugin puts those privileges into the cluster.

## What this requires of your applications

Applications must authenticate to Elasticsearch as the end user, and the username they authenticate
as must be the username Gravitino granted on.

Elasticsearch decides access from the identity presented on the request. If an application connects
with a shared service account, Elasticsearch sees that one identity for every user behind it, and
every one of them gets that account's privileges. The roles Gravitino writes are still correct, and
they still enforce, but they enforce against the service account rather than against the person.
Per-user privileges are not approximated in that setup; they do not apply at all.

Names have to agree as well. If Gravitino grants to `mark` and the token asserts
`mark.hoerth@example.com`, the rule matches nobody. Nothing errors in either system: the grant
succeeds, the role and the mapping are valid, and the user simply has no access.

Worth settling before installing:

- How do applications authenticate to the cluster today, as the end user or as a service account?
- Which realm will the cluster run, and which claim becomes the username?
- Does that value match the username Gravitino holds for the same person, and do group names match?


## The model

Gravitino is the policy authority. Elasticsearch is the enforcement point. No query engine sits in
the request path: applications connect straight to the cluster, and Elasticsearch makes every
access decision itself, using roles that Gravitino wrote and keeps current.

Nothing in the read path goes through Gravitino, so the cluster continues to enforce these
privileges whether or not Gravitino is running. Gravitino's involvement is at grant time and on the
reconciliation interval.

## What is pushed down

A Gravitino grant is three separate things, and each maps onto a different part of an Elasticsearch
role. Taking them in turn.

### 1. The role, and who holds it

| Gravitino | Elasticsearch |
| --- | --- |
| A role | A role named `gravitino_<metalake>_<role>` |
| That role granted to a user | A role mapping rule on `username`, plus the role added to a native user record of that name if one exists |
| That role granted to a group | A role mapping rule on the `groups` claim |

The role holds the privileges. The mapping decides who picks them up at authentication time.

### 2. What the privilege applies to

Elasticsearch has no catalogs or schemas, only a flat namespace of indices, so the Gravitino
hierarchy flattens into index names and patterns.

| Granted on | Becomes |
| --- | --- |
| The catalog, or a schema | An index pattern, `<index.prefix>*` |
| One table | That exact index name |

A pattern covers indices created later. An exact name does not.

### 3. The privilege itself

| Gravitino privilege | Elasticsearch index privileges |
| --- | --- |
| `SELECT_TABLE` | `read`, `view_index_metadata` |
| `MODIFY_TABLE` | `write`, `view_index_metadata` |
| `CREATE_TABLE` | `create_index` |
| `USE_CATALOG`, `USE_SCHEMA` | `view_index_metadata` |

Privileges with no Elasticsearch counterpart are dropped rather than approximated.

### Putting the three together

Granting `USE_CATALOG`, `USE_SCHEMA` and `SELECT_TABLE` on the catalog `search`, in the metalake
`demo`, with `index.prefix` set to `demo_`, and granting that role to the user `mark`, produces this
in Elasticsearch:

```
PUT _security/role/gravitino_demo_analyst
{
  "indices": [
    { "names": ["demo_*"], "privileges": ["read", "view_index_metadata"] }
  ],
  "metadata": { "managed_by": "gravitino", "gravitino_metalake": "demo",
                "gravitino_role": "ANALYST" }
}

PUT _security/role_mapping/gravitino_demo_analyst__mapping
{
  "roles": ["gravitino_demo_analyst"],
  "rules": { "any": [ { "field": { "username": "mark" } } ] }
}
```

The role name comes from part 1, `demo_*` from part 2, the two privileges from part 3.

### Row filters and column restrictions

Gravitino has no privilege that expresses a row filter or a column restriction, and no policy type
wired to one. The plugin therefore takes them from properties set on the role. Treat the property
keys as temporary: a privilege or a policy type is the natural home for this, and adding one would
change how these are expressed. What Elasticsearch does with them would not change.

| Role property | Becomes |
| --- | --- |
| `elasticsearch.dls.<index>`, or `elasticsearch.dls.*` for every index in the role | A document-level security query on that index entry, filtering which documents are visible |
| `elasticsearch.fls.<index>`, or `elasticsearch.fls.*` | A field-level security exception list, removing those fields |

Properties are set when the role is created, alongside its securable objects:

```
POST /api/metalakes/demo/roles
{
  "name": "REGIONAL_ANALYST",
  "properties": {
    "elasticsearch.dls.*": "{\"term\":{\"region\":\"west\"}}",
    "elasticsearch.fls.demo_claims": "ssn, member.dob"
  },
  "securableObjects": [
    {"fullName": "search", "type": "CATALOG",
     "privileges": [{"name": "USE_CATALOG", "condition": "ALLOW"},
                    {"name": "USE_SCHEMA", "condition": "ALLOW"},
                    {"name": "SELECT_TABLE", "condition": "ALLOW"}]}
  ]
}
```

The value of a `dls` property is a Query DSL object as a JSON string. The value of an `fls` property
is a comma separated list of field paths. That role produces this in Elasticsearch:

```
{
  "indices": [
    {
      "names": ["demo_*"],
      "privileges": ["read", "view_index_metadata"],
      "query": { "term": { "region": "west" } }
    }
  ]
}
```

Elasticsearch stores the query as a string, so reading the role back shows it quoted
rather than as an object. Send it as an object.

with the `field_security` block added on the entry for `demo_claims`:

```
"field_security": { "grant": ["*"], "except": ["ssn", "member.dob"] }
```

A document-level security query can be templated on the caller, so one role serves every region:

```
"elasticsearch.dls.*": "{\"term\":{\"region\":\"{{_user.metadata.region}}\"}}"
```

With a JWT realm, token claims are available under `_user.metadata`, so an attribute held in the
identity provider drives the filter.

Three constraints:

- Both require a paid Elastic subscription tier. On a basic license the cluster rejects the role.
- A field removed by field-level security is absent from results and cannot be queried, sorted or
  aggregated, so nothing leaks through a filter. Elasticsearch removes the field rather than masking
  its value, so a partial reveal or a hash needs a masked copy written at ingest.
- Elasticsearch does not permit document updates on an index under either restriction, so the
  plugin rejects a role carrying both write access and a restriction on the same index. Keep read
  roles and write roles separate.

A row filter expressed here must be a Query DSL object. Functions, subqueries and joins have no
equivalent.

## Usage privileges are required

Gravitino does not grant table access on a data privilege alone. Its expression for loading a table
reads:

```
ANY(OWNER, METALAKE, CATALOG) ||
SCHEMA_OWNER_WITH_USE_CATALOG ||
ANY_USE_CATALOG && ANY_USE_SCHEMA && (TABLE::OWNER || ANY_SELECT_TABLE || ANY_MODIFY_TABLE)
```

A role holding only `SELECT_TABLE` therefore grants nothing in Gravitino, and the plugin applies the
same rule: data privileges are pushed only when the role also carries `USE_CATALOG` at metalake or
catalog level and `USE_SCHEMA` at metalake, catalog or schema level. Privileges skipped for that
reason are logged, and the role is still written carrying whatever its usage privileges grant.

The plugin is stricter than Gravitino in two cases, both erring toward less access:

- Gravitino evaluates the expression against everything a user holds, so `USE_CATALOG` in one role
  and `SELECT_TABLE` in another satisfies it. The plugin translates one role at a time, so keep the
  usage privileges in the same role as the data privileges.
- The ownership branches have no Elasticsearch equivalent, so access that depends on owning the
  metalake, catalog or schema is not pushed.

## Deny

Elasticsearch roles only add privileges. No entry in a role subtracts from another, so a deny cannot
be written directly. Gravitino realises it by computing the indices the role still reaches and
writing those in place of the pattern.

A catalog grant of `SELECT_TABLE` with a deny on one index therefore produces explicit entries for
the indices that remain, and the denied index appears nowhere in the role.

One consequence: an index created after that point is not in the list, so it stays outside the role
until the next reconciliation pass, which runs on
`authorization.elasticsearch.reconcile.interval-seconds`.

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
account and assign `gravitino_<metalake>_<role>` to it, and from then on grants and revokes in
Gravitino reach that user through the role itself.

Set `authorization.elasticsearch.native-user.bind` to `false` to skip the native-user handling
on clusters that use external realms only.

### Two things to plan for

**Names must agree.** If Gravitino grants to `mark` and the token asserts
`mark.hoerth@example.com`, the rule matches nobody. Nothing errors: the grant succeeds, the role
and mapping are valid, and the user simply has no access.

**Applications must authenticate as the end user.** If they connect with a shared service account,
Elasticsearch sees one identity and every user of that application gets that account's privileges.
Per-user identity is what makes any of this enforce anything.

**Hand-assigned roles are invisible to Gravitino.** Assigning a `gravitino_*` role to a user
directly grants that user the role's privileges, and Gravitino has no record of it. Reconciliation
rewrites what a role contains, never who holds it.

## Licensing

Index privileges work on any Elastic subscription level. Document-level and field-level security,
and the external realms, require a paid tier. The plugin logs the cluster's license level when the
catalog loads, and warns when it reports `basic`.

## What has been tested

Both components, against Elasticsearch 9.1.0 and 8.15.0 on a trial license: a single node over HTTP,
authenticating against the native realm. The unit and integration suites, a script that exercises
Elasticsearch's own behaviour, a walkthrough of the privilege lifecycle, and the row filter and
column restriction path all pass on both versions, so the security API these components use behaves
the same across that major boundary.

Not exercised, and worth confirming in your environment:

- TLS with a private certificate authority. Verification is either on or skipped, so a private CA
  needs its certificate in the JVM trust store of the Gravitino server.
- A multi-node cluster.
- Any external realm. Role mappings are written and read back correctly, but no JWT, OIDC or SAML
  realm has resolved one, so the claim-to-username mapping described above is by design rather than
  by observation.
- A basic license beyond the warning the plugin logs. Index privileges should work there; row
  filters and column restrictions will be rejected by the cluster.

## What is not included

- Value masking on read. Field-level security removes a field rather than transforming it, so
  partial reveal or hashing needs a masked copy written at ingest and granted in place of the raw
  field.
- Row filters using functions, subqueries or joins. The translatable subset is comparisons, IN,
  AND/OR, ranges and null checks.
- Writes to indices under row or column restrictions, which Elasticsearch does not permit.
- A trust store option. TLS verification is either on or skipped
  (`elasticsearch.tls.skip-verify`, `authorization.elasticsearch.tls.insecure`), so a private
  certificate authority needs the certificate in the JVM trust store of the Gravitino server.
OVERVIEW_EOF
sed -i "s/@VERSION@/$TARGET_VERSION/g" "$STAGE/OVERVIEW.md"

cat > "$STAGE/INSTALL.md" <<'INSTALL_EOF'
# Installing

Six steps, about twenty minutes. Every step ends with a way to confirm it worked before moving on.

Before starting, read the first section of OVERVIEW.md. Elasticsearch enforces against the identity
on the request, so if applications connect with a shared service account, per-user privileges will
not apply no matter how this is installed.

## Step 1. Check the server version

The bundle carries Gravitino api and core jars, so the server must be running @VERSION@ exactly.
A mismatch fails when the catalog loads.

With the server running:

```
curl -s http://your-gravitino-host:8090/api/version
```

If it is stopped, start it first with `$GRAVITINO_HOME/bin/gravitino.sh start`, or read the
version from the banner it prints on startup.

## Step 2. Stop the server and unpack

The commands below use `$GRAVITINO_HOME`, so point it at the installation directory first:

```
export GRAVITINO_HOME=/path/to/your/gravitino
```

```
tar xzf @KIT@.tar.gz
cd @KIT@
cp -r install/catalogs/elasticsearch       $GRAVITINO_HOME/catalogs/
cp -r install/authorizations/elasticsearch $GRAVITINO_HOME/authorizations/
```

Confirm both landed:

```
ls $GRAVITINO_HOME/catalogs/elasticsearch/libs
ls $GRAVITINO_HOME/authorizations/elasticsearch/libs
```

Each should hold a jar named for the component plus its dependencies. Confirm the version matches
the running server:

```
ls $GRAVITINO_HOME/authorizations/elasticsearch/libs | grep gravitino-core
```

That should report `gravitino-core-@VERSION@.jar`.

## Step 3. Enable authorization

Privilege pushdown needs authorization switched on in `$GRAVITINO_HOME/conf/gravitino.conf`.
Both keys are already in that file, so edit the existing lines rather than adding new ones:

```
grep -n gravitino.authorization $GRAVITINO_HOME/conf/gravitino.conf
```

Set the first to true:

```
gravitino.authorization.enable = true
```

The second, `gravitino.authorization.serviceAdmins`, names the accounts that administer access
control. It ships as `anonymous`, which suits a server with no authentication configured. Change
it only if your server authenticates its callers.

Without this the role and permission endpoints return an error and nothing reaches Elasticsearch.

## Step 4. Start the server

```
$GRAVITINO_HOME/bin/gravitino.sh start
curl -s http://your-gravitino-host:8090/api/version
```

## Step 5. Create a governed catalog

The catalog lives in a metalake, so create one first if you do not already have it:

```
curl -s -X POST -H 'Content-Type: application/json' \
  -H 'Accept: application/vnd.gravitino.v1+json' \
  -d '{"name":"your-metalake","comment":"","properties":{}}' \
  http://your-gravitino-host:8090/api/metalakes
```

One catalog carries the connector properties and the authorization properties together, so the same
catalog both federates the cluster and governs it. Substitute your own cluster URL and credentials:

```
curl -s -X POST -H 'Content-Type: application/json' \
  -H 'Accept: application/vnd.gravitino.v1+json' \
  -d '{
        "name": "search",
        "type": "RELATIONAL",
        "provider": "elasticsearch",
        "properties": {
          "elasticsearch.hosts": "https://es.example.com:9200",
          "elasticsearch.username": "your-account",
          "elasticsearch.password": "your-password",

          "authorization-provider": "elasticsearch",
          "authorization.elasticsearch.url": "https://es.example.com:9200",
          "authorization.elasticsearch.auth.type": "basic",
          "authorization.elasticsearch.username": "your-account",
          "authorization.elasticsearch.password": "your-password",
          "authorization.elasticsearch.index.prefix": "",
          "authorization.elasticsearch.reconcile.interval-seconds": "300"
        }
      }' \
  http://your-gravitino-host:8090/api/metalakes/your-metalake/catalogs
```

The account the plugin authenticates as needs `manage_security`. The full property list is in
`source/authorizations/authorization-elasticsearch/README.md`.

`index.prefix` scopes catalog-level grants. With a prefix they become the pattern `<prefix>*`; with
an empty prefix a catalog grant covers every index on the cluster.

## Step 6. Confirm both halves are live

The connector, by listing indices as tables:

```
curl -s -H 'Accept: application/vnd.gravitino.v1+json' \
  http://your-gravitino-host:8090/api/metalakes/your-metalake/catalogs/search/schemas/default/tables
```

The plugin, by the line it logs as the catalog loads:

```
grep "Elasticsearch cluster reports license type" $GRAVITINO_HOME/logs/gravitino-server.log
```

No such line means the plugin did not load, and grants will not reach Elasticsearch. A `basic`
license enforces index privileges but not row filters or field restrictions.

Next, work through VERIFY.md.

## Rebuilding from source

`source/` holds both modules. Copy them into a Gravitino @VERSION@ source tree at the same paths,
add the authorization module to `settings.gradle.kts`, and build:

```
./gradlew :catalogs:catalog-elasticsearch:build
./gradlew :authorizations:authorization-elasticsearch:build
./gradlew compileDistribution -x test
```
INSTALL_EOF
sed -i "s/@VERSION@/$TARGET_VERSION/g; s/@KIT@/$KIT/g" "$STAGE/INSTALL.md"

cat > "$STAGE/VERIFY.md" <<'VERIFY_EOF'
# Verifying the installation

A privilege set in Gravitino, observed in Elasticsearch, then enforced against a real query. The
example uses a native Elasticsearch user, which needs no identity provider, and takes a few minutes.

Substitute your own cluster URL, credentials, metalake name and index names throughout. The example
assumes a catalog named `search` created as in INSTALL.md, with `index.prefix` set to `demo_`,
and two indices `demo_claims` and `demo_pii`.

## 1. Confirm the catalog sees the indices

```
curl -s -H 'Accept: application/vnd.gravitino.v1+json' \
  https://gravitino.example.com:8090/api/metalakes/demo/catalogs/search/schemas/default/tables
```

Each index appears as a table under the synthetic schema `default`.

## 2. Create a role with a read privilege on the catalog

`USE_CATALOG` and `USE_SCHEMA` accompany the data privilege because Gravitino requires them before
a table read is allowed.

```
curl -s -X POST -H 'Content-Type: application/json' \
  -H 'Accept: application/vnd.gravitino.v1+json' \
  -d '{
        "name": "ANALYST",
        "properties": {},
        "securableObjects": [
          {"fullName": "search", "type": "CATALOG",
           "privileges": [{"name": "USE_CATALOG", "condition": "ALLOW"},
                          {"name": "USE_SCHEMA", "condition": "ALLOW"},
                          {"name": "SELECT_TABLE", "condition": "ALLOW"}]}
        ]
      }' \
  https://gravitino.example.com:8090/api/metalakes/demo/roles
```

## 3. Read the role back from Elasticsearch

```
curl -s -u <account>:<password> \
  https://es.example.com:9200/_security/role/gravitino_demo_analyst
```

The catalog grant became the index pattern `demo_*` with the privileges `read` and
`view_index_metadata`. The role name is lower-cased and carries the metalake.

## 4. Deny one index, and see the pattern resolve

```
curl -s -X PUT -H 'Content-Type: application/json' \
  -H 'Accept: application/vnd.gravitino.v1+json' \
  -d '{"privileges":[{"name":"SELECT_TABLE","condition":"DENY"}]}' \
  https://gravitino.example.com:8090/api/metalakes/demo/permissions/roles/ANALYST/table/search.default.demo_pii/grant

curl -s -u <account>:<password> \
  https://es.example.com:9200/_security/role/gravitino_demo_analyst
```

The pattern is gone, replaced by explicit entries for the indices that remain allowed.
`demo_pii` appears nowhere in the role.

## 5. Create a local Elasticsearch user

```
curl -s -u <account>:<password> -X POST \
  -H 'Content-Type: application/json' \
  -d '{"password":"<a password>","roles":["kibana_user"]}' \
  https://es.example.com:9200/_security/user/mark
```

The user carries one unrelated role, which the next step must leave intact.

## 6. Grant the role to that user in Gravitino

```
curl -s -X POST -H 'Content-Type: application/json' \
  -H 'Accept: application/vnd.gravitino.v1+json' \
  -d '{"name":"mark"}' \
  https://gravitino.example.com:8090/api/metalakes/demo/users

curl -s -X PUT -H 'Content-Type: application/json' \
  -H 'Accept: application/vnd.gravitino.v1+json' \
  -d '{"roleNames":["ANALYST"]}' \
  https://gravitino.example.com:8090/api/metalakes/demo/permissions/users/mark/grant
```

## 7. Confirm the binding

```
curl -s -u <account>:<password> https://es.example.com:9200/_security/user/mark
```

The user now holds `kibana_user` and `gravitino_demo_analyst`. No password appears in the
record, because the update left it alone:

```
curl -s -u mark:<a password> https://es.example.com:9200/_security/_authenticate
```

## 8. Query as that user

```
curl -s -u mark:<a password> "https://es.example.com:9200/demo_claims/_search?size=1"
curl -s -u mark:<a password> "https://es.example.com:9200/demo_pii/_search?size=1"
```

The first returns a hit. The second is refused with a 403 naming the effective roles:

```
action [indices:data/read/search] is unauthorized for user [mark] with effective roles
[gravitino_demo_analyst,kibana_user] on indices [demo_pii]
```

Elasticsearch refused the query itself, using a privilege that was set in Gravitino and never
touched in Elasticsearch by hand.

## 8b. Row filters and column restrictions

Requires a paid Elastic subscription tier. Skip on a basic license.

Create a second role carrying a filter and a restriction, and grant it to a second local user:

```
curl -s -X POST -H 'Content-Type: application/json' \
  -H 'Accept: application/vnd.gravitino.v1+json' \
  -d '{
        "name": "REGIONAL",
        "properties": {
          "elasticsearch.dls.*": "{\"term\":{\"region\":\"west\"}}",
          "elasticsearch.fls.*": "ssn"
        },
        "securableObjects": [
          {"fullName": "search", "type": "CATALOG",
           "privileges": [{"name": "USE_CATALOG", "condition": "ALLOW"},
                          {"name": "USE_SCHEMA", "condition": "ALLOW"},
                          {"name": "SELECT_TABLE", "condition": "ALLOW"}]}
        ]
      }' \
  https://gravitino.example.com:8090/api/metalakes/demo/roles
```

Read the role back from Elasticsearch and the index entry now carries a `query` and a
`field_security` block:

```
curl -s -u <account>:<password> \
  https://es.example.com:9200/_security/role/gravitino_demo_regional
```

Create a local user holding only that role, then query as them:

```
curl -s -u <account>:<password> -X POST \
  -H 'Content-Type: application/json' \
  -d '{"password":"<a password>","roles":["gravitino_demo_regional"]}' \
  https://es.example.com:9200/_security/user/regional

curl -s -u regional:<a password> "https://es.example.com:9200/demo_claims/_search"
```

Only documents matching the filter come back, and no `ssn` field appears in any of them. The
restriction holds for aggregations as well as searches:

```
curl -s -u regional:<a password> -X POST -H 'Content-Type: application/json' \
  -d '{"size":0,"aggs":{"regions":{"terms":{"field":"region"}}}}' \
  "https://es.example.com:9200/demo_claims/_search"
```

One bucket, for the region the filter allows.

A caution worth seeing once: Elasticsearch roles union, so a user who also holds an unrestricted
role on the same index sees everything. The restricted role does not act as a ceiling.

```
curl -s -u <account>:<password> -X PUT \
  -H 'Content-Type: application/json' \
  -d '{"roles":["gravitino_demo_regional","gravitino_demo_analyst"]}' \
  https://es.example.com:9200/_security/user/regional

curl -s -u regional:<a password> "https://es.example.com:9200/demo_claims/_search"
```

Every document returns, `ssn` included. Design roles so a governed index is never reachable through
an unrestricted role.

## 9. Clean up

```
curl -s -u <account>:<password> -X DELETE https://es.example.com:9200/_security/user/mark
curl -s -u <account>:<password> -X DELETE https://es.example.com:9200/_security/user/regional
curl -s -X DELETE -H 'Accept: application/vnd.gravitino.v1+json' \
  https://gravitino.example.com:8090/api/metalakes/demo/roles/ANALYST
curl -s -X DELETE -H 'Accept: application/vnd.gravitino.v1+json' \
  https://gravitino.example.com:8090/api/metalakes/demo/roles/REGIONAL
```

Deleting the role in Gravitino removes the Elasticsearch role and its mapping.

## Checking Elasticsearch behaviour on its own

`source/authorizations/authorization-elasticsearch/dev/es-authz-smoke.sh` writes the same role
shapes directly to a cluster, with no Gravitino involved, and reports what Elasticsearch does with
them: index privileges, field restrictions, row filters, and the effect of a second unrestricted
role. It reports the license tier, skips the paid-tier checks on basic, and cleans up after itself.

```
ES_URL=https://es.example.com:9200 ES_USER=<account> ES_PASSWORD=<password> ./es-authz-smoke.sh
```
VERIFY_EOF
sed -i "s/@VERSION@/$TARGET_VERSION/g" "$STAGE/VERIFY.md"

cat > "$STAGE/README.md" <<'README_EOF'
# Elasticsearch connector and privilege pushdown for Gravitino @VERSION@

Two components that let Gravitino govern an Elasticsearch cluster directly, with no query engine in
the path. Prototypes provided for evaluation, not a supported product.

## Read these in order

1. **OVERVIEW.md** explains what the two components do and what gets pushed into Elasticsearch.
   Start with its first section, which covers the one prerequisite that decides whether any of this
   enforces anything in your environment. Settle that before installing.
2. **INSTALL.md** covers the install, the server configuration it needs, and creating a catalog that
   both federates and governs the cluster. About twenty minutes.
3. **VERIFY.md** is a walkthrough: set a privilege in Gravitino, watch it appear in Elasticsearch,
   then watch Elasticsearch refuse a query on the strength of it. About ten minutes, and worth doing
   before anything else is built on top.

## What else is here

- `install/` holds the jars for both components, which is what INSTALL.md copies into place.
- `source/` holds the source of both modules, their build files, a README for each, and a script
  that exercises Elasticsearch's own behaviour independently of Gravitino.
README_EOF
sed -i "s/@VERSION@/$TARGET_VERSION/g" "$STAGE/README.md"

echo "== Archiving =="
rm -f "$ARCHIVE"
(cd "$(dirname "$STAGE")" && tar czf "$ARCHIVE" "$KIT")
(cd "$(dirname "$ARCHIVE")" && sha256sum "$(basename "$ARCHIVE")" > "$(basename "$ARCHIVE").sha256")

echo
echo "Wrote $ARCHIVE"
du -h "$ARCHIVE"
tar tzf "$ARCHIVE" | grep -vE "libs/|/src/" | head -30
