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
# Demonstrates Gravitino privilege pushdown into Elasticsearch. Every privilege is created and
# changed through the Gravitino REST API, and every effect is shown by calling Elasticsearch
# directly. Nothing in the demo talks to Elasticsearch through Gravitino.
#
#   ./demo-es-pushdown.sh setup     build, start the server, create the metalake and catalog
#   ./demo-es-pushdown.sh demo      run the six demo steps, pausing between each
#   ./demo-es-pushdown.sh teardown  stop the server and remove what the demo created
#
# Environment:
#   ES_URL, ES_USER, ES_PASSWORD   the Elasticsearch cluster (defaults to the local container)
#   GRAVITINO_URL                  the Gravitino server (default http://localhost:8090)
#   NO_PAUSE=1                     do not wait for a keypress between steps

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PACKAGE_DIR="$REPO_ROOT/distribution/package"

ES_URL="${ES_URL:-http://localhost:9200}"
ES_USER="${ES_USER:-elastic}"
ES_PASSWORD="${ES_PASSWORD:-smoketest1}"
GRAVITINO_URL="${GRAVITINO_URL:-http://localhost:8090}"

METALAKE="demo"
CATALOG="search"
INDEX_PREFIX="demo_"
ANALYST="ANALYST"
ADMIN_USER="mhoerth"
ANALYST_GROUP="claims-analysts"

BOLD=$'\033[1m'; DIM=$'\033[2m'; GREEN=$'\033[32m'; RESET=$'\033[0m'

banner() { printf '\n%s== %s ==%s\n' "$BOLD" "$1" "$RESET"; }
note()   { printf '%s%s%s\n' "$DIM" "$1" "$RESET"; }
ok()     { printf '%s%s%s\n' "$GREEN" "$1" "$RESET"; }

pause() {
  if [[ "${NO_PAUSE:-0}" != "1" ]]; then
    printf '\n%s[enter to continue]%s ' "$DIM" "$RESET"
    read -r _
  fi
}

es() {
  local method=$1 path=$2 body=${3:-}
  if [[ -n "$body" ]]; then
    curl -s -u "$ES_USER:$ES_PASSWORD" -X "$method" -H 'Content-Type: application/json' \
      -d "$body" "$ES_URL$path"
  else
    curl -s -u "$ES_USER:$ES_PASSWORD" -X "$method" "$ES_URL$path"
  fi
}

grav() {
  local method=$1 path=$2 body=${3:-}
  if [[ -n "$body" ]]; then
    curl -s -X "$method" -H 'Content-Type: application/json' -H 'Accept: application/vnd.gravitino.v1+json' \
      -d "$body" "$GRAVITINO_URL$path"
  else
    curl -s -X "$method" -H 'Accept: application/vnd.gravitino.v1+json' "$GRAVITINO_URL$path"
  fi
}

pretty() {
  if command -v jq >/dev/null 2>&1; then jq .; else cat; fi
}

es_role_name() { printf 'gravitino_%s_%s' "$METALAKE" "$1"; }

show_es_role() {
  local role
  role="$(es_role_name "$1")"
  printf '%sGET %s/_security/role/%s%s\n' "$DIM" "$ES_URL" "$role" "$RESET"
  es GET "/_security/role/$role" | pretty
}

show_es_mapping() {
  local mapping
  mapping="$(es_role_name "$1")__mapping"
  printf '%sGET %s/_security/role_mapping/%s%s\n' "$DIM" "$ES_URL" "$mapping" "$RESET"
  es GET "/_security/role_mapping/$mapping" | pretty
}

# ---------------------------------------------------------------------------- setup

setup() {
  banner "Checking Elasticsearch"
  local license
  license="$(es GET /_license | grep -o '"type" *: *"[^"]*"' | head -1 | sed 's/.*"\([^"]*\)"$/\1/')"
  if [[ -z "$license" ]]; then
    echo "Cannot reach Elasticsearch at $ES_URL" >&2
    exit 1
  fi
  ok "license: $license"
  if [[ "$license" == "basic" ]]; then
    note "Index privileges will work. Row filters and field restrictions will not."
  fi

  banner "Creating demo indices"
  for index in "${INDEX_PREFIX}claims" "${INDEX_PREFIX}members" "${INDEX_PREFIX}pii"; do
    es DELETE "/$index" >/dev/null 2>&1 || true
    es PUT "/$index" '{"mappings":{"properties":{"region":{"type":"keyword"}}}}' >/dev/null
    es POST "/$index/_doc?refresh=true" '{"region":"west"}' >/dev/null
  done
  ok "created ${INDEX_PREFIX}claims, ${INDEX_PREFIX}members, ${INDEX_PREFIX}pii"

  banner "Building the distribution"
  note "This takes a few minutes the first time."
  (cd "$REPO_ROOT" && ./gradlew compileDistribution -x test)
  (cd "$REPO_ROOT" && ./gradlew :authorizations:authorization-elasticsearch:copyLibAndConfig)
  if [[ ! -f "$PACKAGE_DIR/authorizations/elasticsearch/libs/gravitino-authorization-elasticsearch"*.jar ]]; then
    ls "$PACKAGE_DIR/authorizations/elasticsearch/libs" >/dev/null 2>&1 || {
      echo "The plugin jars are not staged under $PACKAGE_DIR/authorizations/elasticsearch/libs" >&2
      exit 1
    }
  fi
  ok "plugin staged at $PACKAGE_DIR/authorizations/elasticsearch/libs"

  banner "Starting the Gravitino server"
  (cd "$PACKAGE_DIR" && ./bin/gravitino.sh restart)
  local attempt=0
  until curl -sf "$GRAVITINO_URL/api/version" >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [[ "$attempt" -gt 60 ]]; then
      echo "The server did not come up. Check $PACKAGE_DIR/logs/gravitino-server.log" >&2
      exit 1
    fi
    sleep 2
  done
  ok "server is up at $GRAVITINO_URL"

  banner "Creating the metalake and the governed catalog"
  grav POST "/api/metalakes" \
    "{\"name\":\"$METALAKE\",\"comment\":\"pushdown demo\",\"properties\":{}}" >/dev/null || true

  local catalog_body
  catalog_body=$(cat <<JSON
{
  "name": "$CATALOG",
  "type": "MODEL",
  "provider": "model",
  "comment": "Governs the Elasticsearch cluster",
  "properties": {
    "authorization-provider": "elasticsearch",
    "authorization.elasticsearch.url": "$ES_URL",
    "authorization.elasticsearch.auth.type": "basic",
    "authorization.elasticsearch.username": "$ES_USER",
    "authorization.elasticsearch.password": "$ES_PASSWORD",
    "authorization.elasticsearch.index.prefix": "$INDEX_PREFIX",
    "authorization.elasticsearch.reconcile.interval-seconds": "30"
  }
}
JSON
)
  grav POST "/api/metalakes/$METALAKE/catalogs" "$catalog_body" | pretty

  banner "Confirming the plugin loaded"
  sleep 2
  if grep -q "Elasticsearch cluster reports license type" "$PACKAGE_DIR/logs/gravitino-server.log" 2>/dev/null; then
    ok "the Elasticsearch authorization plugin constructed and reached the cluster"
  else
    echo "The plugin did not log a license line. Check $PACKAGE_DIR/logs/gravitino-server.log" >&2
    note "Without this the demo steps will create roles in Gravitino that never reach Elasticsearch."
    exit 1
  fi

  grav POST "/api/metalakes/$METALAKE/users" "{\"name\":\"$ADMIN_USER\"}" >/dev/null || true
  grav POST "/api/metalakes/$METALAKE/groups" "{\"name\":\"$ANALYST_GROUP\"}" >/dev/null || true
  ok "setup complete, run: $0 demo"
}

# ---------------------------------------------------------------------------- demo

demo() {
  banner "STEP 1  Elasticsearch has no Gravitino roles yet"
  printf '%sGET %s/_security/role/gravitino_*%s\n' "$DIM" "$ES_URL" "$RESET"
  es GET "/_security/role/gravitino_*" | pretty
  note "An empty object. Nothing has been pushed."
  pause

  banner "STEP 2  Create a read privilege through the Gravitino REST API"
  local read_role
  read_role=$(cat <<JSON
{
  "name": "$ANALYST",
  "properties": {},
  "securableObjects": [
    {
      "fullName": "$CATALOG",
      "type": "CATALOG",
      "privileges": [{"name": "SELECT_TABLE", "condition": "ALLOW"}]
    }
  ]
}
JSON
)
  printf '%sPOST %s/api/metalakes/%s/roles%s\n' "$DIM" "$GRAVITINO_URL" "$METALAKE" "$RESET"
  grav POST "/api/metalakes/$METALAKE/roles" "$read_role" | pretty
  pause

  banner "STEP 3  See that privilege in Elasticsearch"
  show_es_role "$ANALYST"
  note "Gravitino wrote this. The catalog grant became the index pattern ${INDEX_PREFIX}*,"
  note "and SELECT_TABLE became read plus view_index_metadata."
  pause

  banner "STEP 4  Grant the role to a user and a group through Gravitino"
  printf '%sPUT %s/api/metalakes/%s/permissions/users/%s/grant%s\n' \
    "$DIM" "$GRAVITINO_URL" "$METALAKE" "$ADMIN_USER" "$RESET"
  grav PUT "/api/metalakes/$METALAKE/permissions/users/$ADMIN_USER/grant" \
    "{\"roleNames\":[\"$ANALYST\"]}" >/dev/null
  grav PUT "/api/metalakes/$METALAKE/permissions/groups/$ANALYST_GROUP/grant" \
    "{\"roleNames\":[\"$ANALYST\"]}" >/dev/null
  ok "granted to user $ADMIN_USER and group $ANALYST_GROUP"
  echo
  show_es_mapping "$ANALYST"
  note "One role mapping, with a username rule and a groups rule."
  note "Elasticsearch stores no users: the groups rule is matched against the caller's token."
  pause

  banner "STEP 5  Add a write privilege to the same role through Gravitino"
  printf '%sPUT %s/api/metalakes/%s/permissions/roles/%s/catalog/%s/grant%s\n' \
    "$DIM" "$GRAVITINO_URL" "$METALAKE" "$ANALYST" "$CATALOG" "$RESET"
  grav PUT "/api/metalakes/$METALAKE/permissions/roles/$ANALYST/catalog/$CATALOG/grant" \
    '{"privileges":[{"name":"MODIFY_TABLE","condition":"ALLOW"}]}' | pretty
  pause

  banner "STEP 6  See the privilege change in Elasticsearch"
  show_es_role "$ANALYST"
  note "The same role now carries write as well. No Gravitino restart, no engine involved."
  pause

  banner "STEP 7  Revoke the privilege through Gravitino"
  grav PUT "/api/metalakes/$METALAKE/permissions/roles/$ANALYST/catalog/$CATALOG/revoke" \
    '{"privileges":[{"name":"MODIFY_TABLE","condition":"ALLOW"}]}' >/dev/null
  ok "revoked MODIFY_TABLE"
  echo
  show_es_role "$ANALYST"
  note "Back to read only."
  pause

  banner "STEP 8  Revoke the role from the user through Gravitino"
  grav PUT "/api/metalakes/$METALAKE/permissions/users/$ADMIN_USER/revoke" \
    "{\"roleNames\":[\"$ANALYST\"]}" >/dev/null
  ok "revoked from $ADMIN_USER"
  echo
  show_es_mapping "$ANALYST"
  note "The username rule is gone. The group still holds the role, so the mapping remains."
  echo
  ok "Every privilege was created and changed in Gravitino. Every effect was read from Elasticsearch."
}

# ---------------------------------------------------------------------------- teardown

teardown() {
  banner "Removing Elasticsearch state"
  es DELETE "/_security/role/$(es_role_name "$ANALYST")" >/dev/null 2>&1 || true
  es DELETE "/_security/role_mapping/$(es_role_name "$ANALYST")__mapping" >/dev/null 2>&1 || true
  for index in "${INDEX_PREFIX}claims" "${INDEX_PREFIX}members" "${INDEX_PREFIX}pii"; do
    es DELETE "/$index" >/dev/null 2>&1 || true
  done
  banner "Stopping the Gravitino server"
  (cd "$PACKAGE_DIR" && ./bin/gravitino.sh stop) || true
  ok "done"
}

case "${1:-}" in
  setup) setup ;;
  demo) demo ;;
  teardown) teardown ;;
  *)
    echo "Usage: $0 {setup|demo|teardown}" >&2
    exit 1
    ;;
esac
