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
# Runs the row filter and column restriction walkthrough from VERIFY.md against a running
# Gravitino server and Elasticsearch cluster, and checks each claim the document makes.
#
#   ./verify-fgac.sh
#
# Assumes the demo setup has already run, so the catalog "search" exists in the metalake "demo"
# with index.prefix demo_, and the indices demo_claims, demo_members and demo_pii are present.

set -uo pipefail

ES_URL="${ES_URL:-http://localhost:9200}"
ES_USER="${ES_USER:-elastic}"
ES_PASSWORD="${ES_PASSWORD:-smoketest1}"
GRAVITINO_URL="${GRAVITINO_URL:-http://localhost:8090}"

METALAKE="demo"
CATALOG="search"
ROLE="REGIONAL"
ES_ROLE="gravitino_demo_regional"
ANALYST_ES_ROLE="gravitino_demo_analyst"
TEST_USER="regional"
TEST_PASSWORD="regionaltest12345"

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

as_test_user() {
  local method=$1 path=$2 body=${3:-}
  if [[ -n "$body" ]]; then
    curl -s -u "$TEST_USER:$TEST_PASSWORD" -X "$method" -H 'Content-Type: application/json' \
      -d "$body" "$ES_URL$path"
  else
    curl -s -u "$TEST_USER:$TEST_PASSWORD" -X "$method" "$ES_URL$path"
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

cleanup() {
  banner "Cleanup"
  es DELETE "/_security/user/$TEST_USER" >/dev/null 2>&1
  grav DELETE "/api/metalakes/$METALAKE/roles/$ROLE" >/dev/null 2>&1
  echo "  removed the test user and the $ROLE role"
}
trap cleanup EXIT

banner "Preconditions"
LICENSE="$(es GET /_license | grep -o '"type" *: *"[^"]*"' | head -1 | sed 's/.*"\([^"]*\)"$/\1/')"
echo "  license: ${LICENSE:-unknown}"
if [[ "$LICENSE" == "basic" ]]; then
  echo "  Document- and field-level security need a paid tier. Nothing to verify here."
  exit 0
fi
if ! curl -sf "$GRAVITINO_URL/api/version" >/dev/null 2>&1; then
  echo "Cannot reach Gravitino at $GRAVITINO_URL. Run the demo setup first." >&2
  exit 1
fi

# The filter needs documents on both sides of it to be meaningful.
es POST "/demo_claims/_doc?refresh=true" '{"region":"west","ssn":"111-11-1111"}' >/dev/null
es POST "/demo_claims/_doc?refresh=true" '{"region":"east","ssn":"222-22-2222"}' >/dev/null
echo "  seeded demo_claims with a west and an east document"

banner "Creating the restricted role through Gravitino"
grav DELETE "/api/metalakes/$METALAKE/roles/$ROLE" >/dev/null 2>&1
ROLE_BODY=$(cat <<JSON
{
  "name": "$ROLE",
  "properties": {
    "elasticsearch.dls.*": "{\"term\":{\"region\":\"west\"}}",
    "elasticsearch.fls.*": "ssn"
  },
  "securableObjects": [
    {"fullName": "$CATALOG", "type": "CATALOG",
     "privileges": [{"name": "USE_CATALOG", "condition": "ALLOW"},
                    {"name": "USE_SCHEMA", "condition": "ALLOW"},
                    {"name": "SELECT_TABLE", "condition": "ALLOW"}]}
  ]
}
JSON
)
RESULT="$(grav POST "/api/metalakes/$METALAKE/roles" "$ROLE_BODY")"
if echo "$RESULT" | grep -q '"code" *: *0'; then
  pass "role created in Gravitino"
else
  fail "role created in Gravitino"
  echo "$RESULT"
  exit 1
fi

sleep 2

banner "The Elasticsearch role carries the filter and the restriction"
ES_ROLE_JSON="$(es GET "/_security/role/$ES_ROLE")"
echo "$ES_ROLE_JSON"
echo
if echo "$ES_ROLE_JSON" | grep -q '"query"'; then
  pass "index entry carries a document-level security query"
else
  fail "index entry carries a document-level security query"
fi
if echo "$ES_ROLE_JSON" | grep -q '"field_security"'; then
  pass "index entry carries a field_security block"
else
  fail "index entry carries a field_security block"
fi
if echo "$ES_ROLE_JSON" | grep -q '"except"'; then
  pass "the restricted field is listed as an exception"
else
  fail "the restricted field is listed as an exception"
fi

banner "Querying as a user holding only that role"
es DELETE "/_security/user/$TEST_USER" >/dev/null 2>&1
es POST "/_security/user/$TEST_USER" \
  "{\"password\":\"$TEST_PASSWORD\",\"roles\":[\"$ES_ROLE\"]}" >/dev/null

SEARCH="$(as_test_user GET "/demo_claims/_search?size=10")"
if echo "$SEARCH" | grep -q '"west"'; then
  pass "documents matching the filter are returned"
else
  fail "documents matching the filter are returned"
  echo "$SEARCH"
fi
if echo "$SEARCH" | grep -q '"east"'; then
  fail "documents outside the filter are hidden"
else
  pass "documents outside the filter are hidden"
fi
if echo "$SEARCH" | grep -q '"ssn"'; then
  fail "the restricted field is absent from _source"
else
  pass "the restricted field is absent from _source"
fi

AGG="$(as_test_user POST "/demo_claims/_search" \
  '{"size":0,"aggs":{"regions":{"terms":{"field":"region"}}}}')"
if echo "$AGG" | grep -q '"east"'; then
  fail "aggregations respect the filter"
  echo "$AGG"
else
  pass "aggregations respect the filter"
fi

TERM="$(as_test_user POST "/demo_claims/_search" \
  '{"query":{"term":{"ssn":"111-11-1111"}}}')"
if echo "$TERM" | grep -qE '"value" *: *0'; then
  pass "the restricted field cannot be queried"
else
  fail "the restricted field cannot be queried"
  echo "$TERM"
fi

banner "Roles union, so an unrestricted role removes the filter"
if es GET "/_security/role/$ANALYST_ES_ROLE" | grep -q "$ANALYST_ES_ROLE"; then
  es PUT "/_security/user/$TEST_USER" \
    "{\"roles\":[\"$ES_ROLE\",\"$ANALYST_ES_ROLE\"]}" >/dev/null
  BOTH="$(as_test_user GET "/demo_claims/_search?size=10")"
  if echo "$BOTH" | grep -q '"east"'; then
    pass "confirmed: the unrestricted role defeats the filter"
  else
    fail "expected the union to remove the filter, but it held"
  fi
  if echo "$BOTH" | grep -q '"ssn"'; then
    pass "confirmed: the unrestricted role also exposes the restricted field"
  else
    fail "expected the union to expose the restricted field, but it stayed hidden"
  fi
else
  echo "  $ANALYST_ES_ROLE does not exist, so the union case is skipped."
  echo "  Run the demo first if you want to see it."
fi

banner "Result"
if [[ "$FAILURES" -gt 0 ]]; then
  echo "$FAILURES check(s) failed"
  exit 1
fi
echo "All checks passed"
