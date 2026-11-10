<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Elasticsearch authorization plugin

Pushes Gravitino roles and grants into the Elasticsearch security model, so that clients querying
Elasticsearch directly are subject to the privileges defined in Gravitino. No query engine sits in
the path: Gravitino writes the cluster's native security configuration and reconciles it, and
Elasticsearch makes every access decision at query time.

This is a prototype. It is not a supported Datastrato product path.

## Enabling it

Set the authorization provider on the Elasticsearch catalog:

```
authorization-provider = elasticsearch
authorization.elasticsearch.url = https://es.example.com:9200
authorization.elasticsearch.auth.type = basic
authorization.elasticsearch.username = elastic
authorization.elasticsearch.password = ...
```

| Property | Default | Meaning |
| --- | --- | --- |
| `authorization.elasticsearch.url` | required | Cluster base URL |
| `authorization.elasticsearch.auth.type` | `basic` | `basic` or `api-key` |
| `authorization.elasticsearch.username` / `.password` | | Required for `basic` |
| `authorization.elasticsearch.api-key` | | Required for `api-key` |
| `authorization.elasticsearch.role.prefix` | `gravitino_` | Prefix marking the roles this plugin owns |
| `authorization.elasticsearch.index.prefix` | empty | Scopes catalog and schema grants to `<prefix>*` |
| `authorization.elasticsearch.reconcile.interval-seconds` | `300` | 0 disables reconciliation |
| `authorization.elasticsearch.request.timeout-ms` | `10000` | Security API request timeout |
| `authorization.elasticsearch.tls.insecure` | `false` | Skip certificate verification, development only |
| `authorization.elasticsearch.api-key.invalidate-on-revoke` | `true` | Invalidate a user's API keys when their grants change |
| `authorization.elasticsearch.native-user.bind` | `true` | Bind roles to native users of the same name when they exist |
| `authorization.elasticsearch.require-usage-privileges` | `true` | Require USE_CATALOG and USE_SCHEMA in the role before pushing data privileges |

The account the plugin authenticates as needs `manage_security`.

## What maps to what

| Gravitino | Elasticsearch |
| --- | --- |
| Role with privileges | Role `gravitino_<metalake>_<role>` |
| Role granted to a user | Role mapping rule on `username` |
| Role granted to a group | Role mapping rule on the `groups` claim |
| Catalog or schema grant | Index pattern `<index.prefix>*` |
| Table or view grant | Exact index name |
| `SELECT_TABLE`, `SELECT_VIEW` | `read`, `view_index_metadata` |
| `MODIFY_TABLE` | `write`, `view_index_metadata` |
| `CREATE_TABLE`, `CREATE_VIEW` | `create_index` |
| `USE_CATALOG`, `USE_SCHEMA`, `PROBE_TABLE_LIKE` | `view_index_metadata` |

Privileges with no Elasticsearch counterpart are dropped rather than approximated.

One role mapping backs each role and holds both the username rule and the groups rule. It is read
back from the cluster before each rewrite, so plugin state does not have to survive a restart.
Elasticsearch stores no users in this model: identity and group membership arrive in the token the
caller presents, which means a group change made only inside Gravitino never reaches the cluster.

## Scope

The plugin pushes down privileges only: index patterns, exact index names, read and write
privileges, role mappings, and deny expansion. Generated index privileges carry only `names` and
`privileges`, which every Elastic license tier enforces, `basic` included.

Row-filter and column-mask pushdown from Gravitino policies is not yet implemented.

## Users and identity

Elasticsearch holds no users in this model. A grant of a role to a user becomes a role mapping rule
on the username, and a grant to a group becomes a rule on the groups claim, both evaluated at
authentication time against whoever just connected. That means the username Gravitino grants on has
to be the username the caller authenticates as, and the same for group names.

Role mappings are only consulted for external realms such as JWT, OIDC and SAML. For a user who
authenticates against the **native realm**, roles live on the user record instead, so the plugin
also binds the role there:

- If a native user of that name exists, the Elasticsearch role is added to its roles on grant and
  removed on revoke. Other roles, the password and the rest of the record are left alone.
- If no such user exists, the role is still written. An administrator can create the account and
  assign `gravitino_<metalake>_<role>` to it, and later grants and revokes on that role reach the
  user through the role itself.

The plugin never creates native users, because it would have to invent a password nobody can use.
Set `authorization.elasticsearch.native-user.bind` to `false` to skip the binding entirely on
clusters that only use external realms.

One consequence worth knowing: assigning a `gravitino_*` role to a native user by hand grants that
user the role's privileges, and Gravitino has no record of it. Reconciliation rewrites what a role
contains, not who holds it.

## Usage privileges

Gravitino does not grant table access on a data privilege alone. Its expression for loading a table
reads:

```
ANY(OWNER, METALAKE, CATALOG) ||
SCHEMA_OWNER_WITH_USE_CATALOG ||
ANY_USE_CATALOG && ANY_USE_SCHEMA && (TABLE::OWNER || ANY_SELECT_TABLE || ANY_MODIFY_TABLE)
```

So a role holding only `SELECT_TABLE` grants nothing in Gravitino. The plugin applies the same rule
before pushing: a role's data privileges reach Elasticsearch only when the role also carries
`USE_CATALOG` at metalake or catalog level and `USE_SCHEMA` at metalake, catalog or schema level.
A role that fails the check is still written, carrying whatever its usage privileges grant, and the
skipped privileges are logged.

Set `authorization.elasticsearch.require-usage-privileges` to `false` to push data privileges
regardless, which makes Elasticsearch more permissive than Gravitino.

Two places where the plugin is stricter than Gravitino, both erring toward less access:

- **Usage and data privileges split across roles.** Gravitino evaluates the expression against
  everything a user holds, so `USE_CATALOG` in one role and `SELECT_TABLE` in another satisfies it.
  The plugin translates one role at a time, so the data privilege is not pushed. Keep the usage
  privileges in the same role as the data privileges.
- **Ownership.** The first two branches of the expression grant access through ownership of the
  metalake, catalog or schema. Elasticsearch has no ownership concept and the plugin does not
  translate owners, so access that depends on ownership alone is not pushed.

## Known limits

- **Deny is realised by expansion.** Elasticsearch roles are additive, so a deny under an allowed
  pattern replaces that pattern with the indices that remain allowed. Indices created afterwards
  stay out of the role until the next reconciliation pass.
- **Roles union.** Elasticsearch grants the union of every role a user holds, so a role assigned
  outside Gravitino, built-in roles included, can widen what a governed user reaches.
- **Hand edits are reverted, not prevented.** A cluster superuser can edit a `gravitino_*` role. The
  reconciliation pass puts it back.

## Verifying against a live cluster

`dev/demo-es-pushdown.sh` drives the plugin through the Gravitino REST API and reads each effect
back from Elasticsearch: role creation, grants to a user and a group, a deny, a privilege change and
revokes. `dev/verify-identity.sh` checks the JWT and native realm identity paths, and
`dev/verify-late-user.sh` checks a grant made before the Elasticsearch account exists.

```
./authorizations/authorization-elasticsearch/dev/demo-es-pushdown.sh setup
NO_PAUSE=1 ./authorizations/authorization-elasticsearch/dev/demo-es-pushdown.sh demo
```

## Building and testing

```
./gradlew :authorizations:authorization-elasticsearch:test
./gradlew :authorizations:authorization-elasticsearch:copyLibAndConfig
```

The second task stages the jars under `distribution/package/authorizations/elasticsearch/libs`,
which is where `BaseAuthorization` looks for them at runtime.
