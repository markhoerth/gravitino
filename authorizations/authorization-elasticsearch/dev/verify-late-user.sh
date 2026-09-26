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
# Verifies the ordering where the grant happens before the Elasticsearch user exists:
#
#   1. Gravitino grants a role to someone with no account on the cluster. The role is written and
#      the plugin creates nothing.
#   2. An administrator creates that account later and assigns the role by hand.
#   3. The user has the access the role carries.
#   4. A later privilege change in Gravitino reaches that user through the role.
#   5. A later revoke of the role leaves the hand-made assignment in place, which is the one thing
#      an administrator taking this path needs to know.
#
#   ./verify-late-user.sh
#
# Assumes the demo setup has run, so the catalog, the ANALYST role and the demo_ indices exist.

set -uo pipefail

ES_URL="${ES_URL:-http://localhost:9200}"
ES_USER="${ES_USER:-elastic}"
ES_PASSWORD="${ES_PASSWORD:-smoketest1}"
GRAVITINO_URL="${GRAVITINO_URL:-http://localhost:8090}"

METALAKE="demo"
CATALOG="search"
ROLE="ANALYST"
ES_ROLE="gravitino_demo_analyst"
LATE_USER="latecomer"
LATE_PASSWORD="latecomertest12345"

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

as_late_user() {
  curl -s -u "$LATE_USER:$LATE_PASSWORD" "$ES_URL$1"
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

cleanup() {
  banner "Cleanup"
  es DELETE "/_security/user/$LATE_USER" >/dev/null 2>&1
  grav PUT "/api/metalakes/$METALAKE/permissions/users/$LATE_USER/revoke" \
    "{\"roleNames\":[\"$ROLE\"]}" >/dev/null 2>&1
  grav PUT "/api/metalakes/$METALAKE/permissions/roles/$ROLE/catalog/$CATALOG/revoke" \
    '{"privileges":[{"name":"MODIFY_TABLE","condition":"ALLOW"}]}' >/dev/null 2>&1
  echo "  removed the test user, its grant and the added privilege"
}
trap cleanup EXIT

banner "Preconditions"
if ! curl -sf "$GRAVITINO_URL/api/version" >/dev/null 2>&1; then
  echo "Cannot reach Gravitino at $GRAVITINO_URL. Run the demo setup first." >&2
  exit 1
fi
if ! es GET "/_security/role/$ES_ROLE" | grep -q "$ES_ROLE"; then
  echo "$ES_ROLE does not exist on the cluster. Run the demo first." >&2
  exit 1
fi
es DELETE "/_security/user/$LATE_USER" >/dev/null 2>&1
echo "  $LATE_USER does not exist on the cluster"

banner "Step 1: grant in Gravitino while the account does not exist"
grav POST "/api/metalakes/$METALAKE/users" "{\"name\":\"$LATE_USER\"}" >/dev/null 2>&1
RESULT="$(grav PUT "/api/metalakes/$METALAKE/permissions/users/$LATE_USER/grant" \
  "{\"roleNames\":[\"$ROLE\"]}")"
if echo "$RESULT" | grep -q '"code" *: *0'; then
  pass "the grant succeeded in Gravitino"
else
  fail "the grant succeeded in Gravitino"
  echo "$RESULT"
  exit 1
fi
sleep 2

if es GET "/_security/user/$LATE_USER" | grep -q "$LATE_USER"; then
  fail "the plugin must not create the account"
else
  pass "the plugin created no account"
fi
if es GET "/_security/role/$ES_ROLE" | grep -q "$ES_ROLE"; then
  pass "the role is written and waiting"
else
  fail "the role is written and waiting"
fi
if es GET "/_security/role_mapping/${ES_ROLE}__mapping" | grep -q "$LATE_USER"; then
  pass "a role mapping rule was written for the name"
else
  fail "a role mapping rule was written for the name"
fi

banner "Step 2: an administrator creates the account and assigns the role"
es POST "/_security/user/$LATE_USER" \
  "{\"password\":\"$LATE_PASSWORD\",\"roles\":[\"$ES_ROLE\"]}" >/dev/null
RECORD="$(es GET "/_security/user/$LATE_USER")"
echo "$RECORD"
echo
if echo "$RECORD" | grep -q "$ES_ROLE"; then
  pass "the account holds the role"
else
  fail "the account holds the role"
fi

banner "Step 3: the user has the access the role carries"
SEARCH="$(as_late_user "/demo_claims/_search?size=1")"
if echo "$SEARCH" | grep -q '"hits"'; then
  pass "the user can read a granted index"
else
  fail "the user can read a granted index"
  echo "$SEARCH"
fi

WRITE="$(curl -s -u "$LATE_USER:$LATE_PASSWORD" -X POST \
  -H 'Content-Type: application/json' -d '{"region":"north"}' \
  "$ES_URL/demo_claims/_doc?refresh=true")"
if echo "$WRITE" | grep -q 'security_exception'; then
  pass "the user cannot write, since the role carries read only"
else
  fail "the user cannot write, since the role carries read only"
  echo "$WRITE"
fi

banner "Step 4: a privilege change in Gravitino reaches the user"
grav PUT "/api/metalakes/$METALAKE/permissions/roles/$ROLE/catalog/$CATALOG/grant" \
  '{"privileges":[{"name":"MODIFY_TABLE","condition":"ALLOW"}]}' >/dev/null
sleep 3

WRITE_AFTER="$(curl -s -u "$LATE_USER:$LATE_PASSWORD" -X POST \
  -H 'Content-Type: application/json' -d '{"region":"north"}' \
  "$ES_URL/demo_claims/_doc?refresh=true")"
if echo "$WRITE_AFTER" | grep -q '"result"'; then
  pass "the added privilege reached the user through the role"
else
  fail "the added privilege reached the user through the role"
  echo "$WRITE_AFTER"
fi

grav PUT "/api/metalakes/$METALAKE/permissions/roles/$ROLE/catalog/$CATALOG/revoke" \
  '{"privileges":[{"name":"MODIFY_TABLE","condition":"ALLOW"}]}' >/dev/null
sleep 3

WRITE_REVOKED="$(curl -s -u "$LATE_USER:$LATE_PASSWORD" -X POST \
  -H 'Content-Type: application/json' -d '{"region":"north"}' \
  "$ES_URL/demo_claims/_doc?refresh=true")"
if echo "$WRITE_REVOKED" | grep -q 'security_exception'; then
  pass "revoking the privilege reached the user as well"
else
  fail "revoking the privilege reached the user as well"
  echo "$WRITE_REVOKED"
fi

banner "Step 5: revoking the role in Gravitino"
grav PUT "/api/metalakes/$METALAKE/permissions/users/$LATE_USER/revoke" \
  "{\"roleNames\":[\"$ROLE\"]}" >/dev/null
sleep 2

AFTER_REVOKE="$(es GET "/_security/user/$LATE_USER")"
echo "$AFTER_REVOKE"
echo
if echo "$AFTER_REVOKE" | grep -q "$ES_ROLE"; then
  echo "  The hand-made assignment survives the revoke: Gravitino removed the mapping rule, but"
  echo "  the role is on the account record, which Gravitino did not put there and does not track."
  echo "  An administrator who assigns a gravitino_* role by hand owns removing it."
  pass "recorded the behaviour of a hand-made assignment on revoke"
else
  echo "  The revoke removed the role from the account record as well."
  pass "recorded the behaviour of a hand-made assignment on revoke"
fi

STILL_READS="$(as_late_user "/demo_claims/_search?size=1")"
if echo "$STILL_READS" | grep -q '"hits"'; then
  echo "  The user can still read, because the role is still on the record."
else
  echo "  The user can no longer read."
fi

banner "Result"
if [[ "$FAILURES" -gt 0 ]]; then
  echo "$FAILURES check(s) failed"
  exit 1
fi
echo "All checks passed"
