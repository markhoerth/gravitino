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
# Verifies both identity paths the plugin supports, against a local container.
#
#   1. A JWT realm. A token asserting a username is resolved by the role mapping the plugin wrote,
#      and the caller gets the Gravitino role without any user record existing in Elasticsearch.
#      A second token carrying only a groups claim exercises the group rule.
#   2. The native realm. A local user of the same name has the role bound onto its record.
#
# The script configures a JWT realm on the container, which needs a restart, then drives Gravitino
# to create and grant a role and checks what each caller actually gets.
#
#   ./verify-identity.sh
#
# Assumes the demo setup has run, so the catalog and the demo_ indices exist.

set -uo pipefail

CONTAINER="${CONTAINER:-es-authz}"
ES_URL="${ES_URL:-http://localhost:9200}"
ES_USER="${ES_USER:-elastic}"
ES_PASSWORD="${ES_PASSWORD:-smoketest1}"
GRAVITINO_URL="${GRAVITINO_URL:-http://localhost:8090}"

METALAKE="demo"
CATALOG="search"
ROLE="ANALYST"
ES_ROLE="gravitino_demo_analyst"
GROUP="claims-analysts"

JWT_USER="jwtuser"
GROUP_USER="groupuser"
NATIVE_USER="mark"
NATIVE_PASSWORD="marktest12345"

ISSUER="https://gravitino-demo/issuer"
AUDIENCE="elasticsearch"
HMAC_KEY="gravitino-demo-hmac-signing-key-at-least-256-bits-long"
SHARED_SECRET="gravitino-demo-client-shared-secret"

FAILURES=0
pass() { printf '  PASS  %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; FAILURES=$((FAILURES + 1)); }
banner() { printf '\n== %s ==\n' "$1"; }

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
    curl -s -X "$method" -H 'Content-Type: application/json' \
      -H 'Accept: application/vnd.gravitino.v1+json' -d "$body" "$GRAVITINO_URL$path"
  else
    curl -s -X "$method" -H 'Accept: application/vnd.gravitino.v1+json' "$GRAVITINO_URL$path"
  fi
}

# Mints an HS256 token. Elasticsearch takes the UTF-8 bytes of the configured key.
mint_token() {
  local subject=$1 groups=$2
  python3 - "$subject" "$groups" "$ISSUER" "$AUDIENCE" "$HMAC_KEY" <<'PYEOF'
import base64, hashlib, hmac, json, sys, time

subject, groups, issuer, audience, key = sys.argv[1:6]

def b64(raw):
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

now = int(time.time())
header = {"alg": "HS256", "typ": "JWT"}
payload = {
    "iss": issuer,
    "aud": audience,
    "sub": subject,
    "iat": now,
    "exp": now + 3600,
}
if groups:
    payload["groups"] = groups.split(",")

signing_input = (b64(json.dumps(header).encode()) + "." + b64(json.dumps(payload).encode()))
signature = hmac.new(key.encode(), signing_input.encode(), hashlib.sha256).digest()
print(signing_input + "." + b64(signature))
PYEOF
}

as_token() {
  local token=$1 path=$2
  curl -s -H "Authorization: Bearer $token" \
    -H "ES-Client-Authentication: SharedSecret $SHARED_SECRET" \
    "$ES_URL$path"
}

cleanup() {
  banner "Cleanup"
  es DELETE "/_security/user/$NATIVE_USER" >/dev/null 2>&1
  grav PUT "/api/metalakes/$METALAKE/permissions/users/$JWT_USER/revoke" \
    "{\"roleNames\":[\"$ROLE\"]}" >/dev/null 2>&1
  grav PUT "/api/metalakes/$METALAKE/permissions/groups/$GROUP/revoke" \
    "{\"roleNames\":[\"$ROLE\"]}" >/dev/null 2>&1
  echo "  removed the native user and the grants"
  echo "  the JWT realm is left configured on $CONTAINER; recreate the container to remove it"
}
trap cleanup EXIT

# ---------------------------------------------------------------- realm setup

banner "Configuring a JWT realm on $CONTAINER"
if es GET "/_nodes/settings?filter_path=nodes.*.settings.xpack.security.authc.realms.jwt" \
    | grep -q "jwt"; then
  echo "  a JWT realm is already configured, skipping the restart"
else
  docker exec "$CONTAINER" bash -c "cat >> config/elasticsearch.yml <<'YML'

xpack.security.authc.realms.jwt.jwt1.order: 2
xpack.security.authc.realms.jwt.jwt1.token_type: id_token
xpack.security.authc.realms.jwt.jwt1.allowed_issuer: $ISSUER
xpack.security.authc.realms.jwt.jwt1.allowed_audiences: $AUDIENCE
xpack.security.authc.realms.jwt.jwt1.claims.principal: sub
xpack.security.authc.realms.jwt.jwt1.claims.groups: groups
xpack.security.authc.realms.jwt.jwt1.client_authentication.type: shared_secret
YML" || { echo "Failed to write the realm configuration" >&2; exit 1; }

  docker exec "$CONTAINER" bash -c \
    "echo -n '$HMAC_KEY' | bin/elasticsearch-keystore add -x -f xpack.security.authc.realms.jwt.jwt1.hmac_key" \
    || { echo "Failed to add the signing key" >&2; exit 1; }
  docker exec "$CONTAINER" bash -c \
    "echo -n '$SHARED_SECRET' | bin/elasticsearch-keystore add -x -f xpack.security.authc.realms.jwt.jwt1.client_authentication.shared_secret" \
    || { echo "Failed to add the shared secret" >&2; exit 1; }

  echo "  restarting the container"
  docker restart "$CONTAINER" >/dev/null
  for _ in $(seq 1 60); do
    if curl -sf -u "$ES_USER:$ES_PASSWORD" "$ES_URL" >/dev/null 2>&1; then break; fi
    sleep 2
  done
fi

if curl -sf -u "$ES_USER:$ES_PASSWORD" "$ES_URL" >/dev/null 2>&1; then
  pass "cluster is back up with the JWT realm configured"
else
  fail "cluster did not come back up"
  exit 1
fi

# ---------------------------------------------------------------- grants

banner "Granting the role through Gravitino"
grav POST "/api/metalakes/$METALAKE/users" "{\"name\":\"$JWT_USER\"}" >/dev/null 2>&1
grav POST "/api/metalakes/$METALAKE/groups" "{\"name\":\"$GROUP\"}" >/dev/null 2>&1
grav POST "/api/metalakes/$METALAKE/users" "{\"name\":\"$NATIVE_USER\"}" >/dev/null 2>&1

# The native user must exist before the grant for the binding to happen.
es DELETE "/_security/user/$NATIVE_USER" >/dev/null 2>&1
es POST "/_security/user/$NATIVE_USER" \
  "{\"password\":\"$NATIVE_PASSWORD\",\"roles\":[\"kibana_user\"]}" >/dev/null

grav PUT "/api/metalakes/$METALAKE/permissions/users/$JWT_USER/grant" \
  "{\"roleNames\":[\"$ROLE\"]}" >/dev/null
grav PUT "/api/metalakes/$METALAKE/permissions/groups/$GROUP/grant" \
  "{\"roleNames\":[\"$ROLE\"]}" >/dev/null
grav PUT "/api/metalakes/$METALAKE/permissions/users/$NATIVE_USER/grant" \
  "{\"roleNames\":[\"$ROLE\"]}" >/dev/null
sleep 2

MAPPING="$(es GET "/_security/role_mapping/${ES_ROLE}__mapping")"
if echo "$MAPPING" | grep -q "$JWT_USER"; then
  pass "the role mapping carries a username rule for $JWT_USER"
else
  fail "the role mapping carries a username rule for $JWT_USER"
  echo "$MAPPING"
fi
if echo "$MAPPING" | grep -q "$GROUP"; then
  pass "the role mapping carries a groups rule for $GROUP"
else
  fail "the role mapping carries a groups rule for $GROUP"
fi

# ---------------------------------------------------------------- path 1: JWT

banner "Path 1: a token asserting a username"
TOKEN="$(mint_token "$JWT_USER" "")"
AUTH="$(as_token "$TOKEN" "/_security/_authenticate")"
echo "$AUTH"
echo
if echo "$AUTH" | grep -q "\"username\" *: *\"$JWT_USER\""; then
  pass "the token authenticated as $JWT_USER"
else
  fail "the token authenticated as $JWT_USER"
fi
if echo "$AUTH" | grep -q "$ES_ROLE"; then
  pass "the role mapping resolved and granted $ES_ROLE"
else
  fail "the role mapping resolved and granted $ES_ROLE"
fi
if echo "$AUTH" | grep -q '"type" *: *"jwt"'; then
  pass "authentication went through the JWT realm"
else
  fail "authentication went through the JWT realm"
fi

SEARCH="$(as_token "$TOKEN" "/demo_claims/_search?size=1")"
if echo "$SEARCH" | grep -q '"hits"'; then
  pass "the token holder can query a granted index"
else
  fail "the token holder can query a granted index"
  echo "$SEARCH"
fi

banner "Path 1b: a token carrying only a groups claim"
GROUP_TOKEN="$(mint_token "$GROUP_USER" "$GROUP")"
GROUP_AUTH="$(as_token "$GROUP_TOKEN" "/_security/_authenticate")"
if echo "$GROUP_AUTH" | grep -q "$ES_ROLE"; then
  pass "a user unknown to Gravitino got the role through the groups claim"
else
  fail "a user unknown to Gravitino got the role through the groups claim"
  echo "$GROUP_AUTH"
fi

GROUP_SEARCH="$(as_token "$GROUP_TOKEN" "/demo_claims/_search?size=1")"
if echo "$GROUP_SEARCH" | grep -q '"hits"'; then
  pass "the group member can query a granted index"
else
  fail "the group member can query a granted index"
fi

banner "Path 1c: a token for someone with no grant"
STRANGER="$(mint_token "stranger" "")"
STRANGER_AUTH="$(as_token "$STRANGER" "/_security/_authenticate")"
if echo "$STRANGER_AUTH" | grep -q "$ES_ROLE"; then
  fail "an ungranted token holder must not get the role"
  echo "$STRANGER_AUTH"
else
  pass "an ungranted token holder does not get the role"
fi

# ---------------------------------------------------------------- path 2: native

banner "Path 2: a native user of the same name"
USER_RECORD="$(es GET "/_security/user/$NATIVE_USER")"
echo "$USER_RECORD"
echo
if echo "$USER_RECORD" | grep -q "$ES_ROLE"; then
  pass "the role was bound onto the native user record"
else
  fail "the role was bound onto the native user record"
fi
if echo "$USER_RECORD" | grep -q "kibana_user"; then
  pass "the user's existing role was preserved"
else
  fail "the user's existing role was preserved"
fi

NATIVE_AUTH="$(curl -s -u "$NATIVE_USER:$NATIVE_PASSWORD" "$ES_URL/_security/_authenticate")"
if echo "$NATIVE_AUTH" | grep -q '"type" *: *"native"'; then
  pass "the native user still authenticates with its own password"
else
  fail "the native user still authenticates with its own password"
fi

NATIVE_SEARCH="$(curl -s -u "$NATIVE_USER:$NATIVE_PASSWORD" "$ES_URL/demo_claims/_search?size=1")"
if echo "$NATIVE_SEARCH" | grep -q '"hits"'; then
  pass "the native user can query a granted index"
else
  fail "the native user can query a granted index"
fi

banner "Result"
if [[ "$FAILURES" -gt 0 ]]; then
  echo "$FAILURES check(s) failed"
  exit 1
fi
echo "All checks passed"
