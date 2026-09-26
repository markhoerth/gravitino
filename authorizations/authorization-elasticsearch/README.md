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

## Row filters and column restrictions

The plugin SPI carries no tag or policy events, so this prototype reads them from role properties:

| Property | Effect |
| --- | --- |
| `elasticsearch.dls.<index>` | Query DSL object applied as document-level security |
| `elasticsearch.dls.*` | The same, for every index in the role |
| `elasticsearch.fls.<index>` | Comma-separated fields, applied as `field_security.except` |
| `elasticsearch.fls.*` | The same, for every index in the role |

Document-level security queries may template on the caller, for example
`{"term":{"region":"{{_user.metadata.region}}"}}`, which lets one role serve every region.

Both require a paid Elastic tier. The plugin logs a warning at startup when the cluster reports a
basic license, and the role descriptors carrying restrictions will be rejected by the cluster.

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

## Known limits

- **Field restrictions hide rather than mask.** Elasticsearch removes the field entirely. Partial
  reveal or hashing needs a masked copy of the field written at ingest and granted in its place.
- **Deny is realised by expansion.** Elasticsearch roles are additive, so a deny under an allowed
  pattern replaces that pattern with the indices that remain allowed. Indices created afterwards
  stay out of the role until the next reconciliation pass.
- **Roles union.** A user holding an unrestricted role beside a restricted one on the same index
  sees everything. No governed user may hold a role, built-in roles included, that touches a
  governed index without its restrictions.
- **Restrictions are read-only.** Elasticsearch restricts document updates on indices under
  document- or field-level security, so the translator rejects a role that carries both write access
  and a restriction.
- **Hand edits are reverted, not prevented.** A cluster superuser can edit a `gravitino_*` role. The
  reconciliation pass puts it back.

## Verifying against a live cluster

`dev/es-authz-smoke.sh` writes the role shapes the translator emits to a real cluster, maps a native
test user onto them, and queries as that user. It reports the license tier, skips the paid-tier
checks on basic, and cleans up after itself.

```
ES_URL=https://localhost:9200 ES_USER=elastic ES_PASSWORD=... \
  ./authorizations/authorization-elasticsearch/dev/es-authz-smoke.sh
```

The last case is deliberately a demonstration rather than a check of the plugin: it shows a second
unrestricted role defeating a row filter, which is the composition trap the plugin has to avoid when
roles are assigned.

## Building and testing

```
./gradlew :authorizations:authorization-elasticsearch:test
./gradlew :authorizations:authorization-elasticsearch:copyLibAndConfig
```

The second task stages the jars under `distribution/package/authorizations/elasticsearch/libs`,
which is where `BaseAuthorization` looks for them at runtime.
