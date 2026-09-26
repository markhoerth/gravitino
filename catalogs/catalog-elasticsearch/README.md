# Elasticsearch catalog connector

Federates an Elasticsearch cluster as a Gravitino catalog. Each index becomes a table and each
field in its mapping becomes a column, so indices can be discovered, tagged, and granted on
alongside the rest of the estate.

Read-only. The connector does not create, alter or drop anything in the cluster.

A prototype, not a supported Datastrato product path.

## Creating a catalog

```
POST /api/metalakes/<metalake>/catalogs
{
  "name": "search",
  "type": "RELATIONAL",
  "provider": "elasticsearch",
  "properties": {
    "elasticsearch.hosts": "https://es.example.com:9200",
    "elasticsearch.username": "<account>",
    "elasticsearch.password": "<password>"
  }
}
```

| Property | Default | Meaning |
| --- | --- | --- |
| `elasticsearch.hosts` | required | Comma separated cluster endpoints, each with an explicit scheme |
| `elasticsearch.api-key` | | Base64 encoded `id:key`, sent as an `Authorization: ApiKey` header. Mutually exclusive with the username |
| `elasticsearch.username` / `.password` | | Basic auth credentials, as an alternative to an API key |
| `elasticsearch.connect-timeout-ms` | `5000` | Connect timeout |
| `elasticsearch.socket-timeout-ms` | `30000` | Socket timeout |
| `elasticsearch.include-system-indices` | `false` | Whether indices whose name starts with a dot are surfaced as tables |
| `elasticsearch.tls.skip-verify` | `false` | Skip certificate and hostname verification, for clusters presenting a self-signed certificate |

Defaults can also be set for every catalog in `catalogs/elasticsearch/conf/elasticsearch.conf`.
Values on an individual catalog override anything declared there.

A private certificate authority is better handled by adding its certificate to the JVM trust store
of the Gravitino server than by skipping verification.

## What the catalog exposes

Elasticsearch has no schema concept, so the connector presents a single synthetic schema named
`default`. Every index appears as a table beneath it, which makes the full name of an index
`<catalog>.default.<index>`. That is the name to use when granting a privilege on one index, or when
associating a tag or policy.

Names are case sensitive, matching Elasticsearch.

Indices beginning with a dot are Elasticsearch's own bookkeeping rather than anything a user
modeled, so they are hidden unless `elasticsearch.include-system-indices` is set.

## Type conversion

Columns come from the index mapping:

| Elasticsearch | Gravitino |
| --- | --- |
| `keyword`, `constant_keyword`, `wildcard`, `text`, `match_only_text`, `annotated_text`, `version`, `ip` | `string` |
| `long`, `unsigned_long` | `long` |
| `integer` | `integer` |
| `short` | `short` |
| `byte` | `byte` |
| `double`, `scaled_float` | `double` |
| `float`, `half_float` | `float` |
| `boolean` | `boolean` |
| `date`, `date_nanos` | `timestamp with time zone` |
| `binary` | `binary` |
| `dense_vector` | `list<float>` |
| `object`, `nested` | struct of the fields beneath |
| anything else | external type, carrying the Elasticsearch type name |

A field type with no faithful Gravitino equivalent is surfaced as an external type rather than
forced into an approximate one, so nothing is silently misrepresented. Field descriptions come from
the `meta.description` entry on the mapping where one is present.

## Supported operations

| Operation | Behaviour |
| --- | --- |
| List and load schemas | The single `default` schema |
| List tables | Every index the account can see, subject to `include-system-indices` |
| Load table | Columns resolved from the index mapping |
| Create, alter, drop schema | Not supported |
| Create, alter, drop, purge table | Not supported |

Index creation and mapping changes stay with whatever already manages the cluster.

## Governing the indices

Listing indices as tables is what makes privileges on them expressible. Enforcing those privileges
in Elasticsearch needs the companion authorization plugin, which writes Gravitino roles and grants
into the cluster's own security configuration. See
`authorizations/authorization-elasticsearch/README.md`.
