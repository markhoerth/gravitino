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
# Validates the role descriptors the Gravitino Elasticsearch authorization plugin emits against a
# live cluster, without a Gravitino server in the picture. It writes the same role shapes the
# translator produces, maps a native test user onto them, and queries as that user.
#
# Usage:
#   ES_URL=https://localhost:9200 ES_USER=elastic ES_PASSWORD=... ./es-authz-smoke.sh
#
# Add CURL_INSECURE=1 for a cluster with a self-signed certificate.

set -euo pipefail

ES_URL="${ES_URL:-https://localhost:9200}"
ES_USER="${ES_USER:-elastic}"
ES_PASSWORD="${ES_PASSWORD:?ES_PASSWORD must be set}"
TEST_USER="gravitino_smoke_user"
TEST_PASSWORD="gravitino_smoke_password_1"
ROLE_PREFIX="gravitino_smoke_"

CURL_OPTS=(-s -S)
if [[ "${CURL_INSECURE:-0}" == "1" ]]; then
  CURL_OPTS+=(-k)
fi

admin() {
  local method=$1 path=$2 body=${3:-}
  if [[ -n "$body" ]]; then
    curl "${CURL_OPTS[@]}" -u "$ES_USER:$ES_PASSWORD" -X "$method" \
      -H 'Content-Type: application/json' -d "$body" "$ES_URL$path"
  else
    curl "${CURL_OPTS[@]}" -u "$ES_USER:$ES_PASSWORD" -X "$method" "$ES_URL$path"
  fi
}

as_test_user() {
  local method=$1 path=$2 body=${3:-}
  if [[ -n "$body" ]]; then
    curl "${CURL_OPTS[@]}" -u "$TEST_USER:$TEST_PASSWORD" -X "$method" \
      -H 'Content-Type: application/json' -d "$body" "$ES_URL$path"
  else
    curl "${CURL_OPTS[@]}" -u "$TEST_USER:$TEST_PASSWORD" -X "$method" "$ES_URL$path"
  fi
}

pass() { printf '  PASS  %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; FAILURES=$((FAILURES + 1)); }
FAILURES=0

echo "== License =="
LICENSE=$(admin GET /_license | grep -o '"type" *: *"[^"]*"' | head -1 | sed 's/.*"\([^"]*\)"$/\1/')
echo "  license type: ${LICENSE:-unknown}"
PAID=1
if [[ "$LICENSE" == "basic" ]]; then
  PAID=0
  echo "  Document- and field-level security are unavailable on this tier. Those checks are skipped."
fi

echo "== Fixtures =="
admin DELETE "/smoke_claims,/smoke_pii" >/dev/null 2>&1 || true
admin PUT /smoke_claims '{"mappings":{"properties":{"region":{"type":"keyword"},"ssn":{"type":"keyword"},"amount":{"type":"double"}}}}' >/dev/null
admin PUT /smoke_pii '{"mappings":{"properties":{"ssn":{"type":"keyword"}}}}' >/dev/null
admin POST '/smoke_claims/_doc?refresh=true' '{"region":"west","ssn":"111-11-1111","amount":100}' >/dev/null
admin POST '/smoke_claims/_doc?refresh=true' '{"region":"east","ssn":"222-22-2222","amount":200}' >/dev/null
admin POST '/smoke_pii/_doc?refresh=true' '{"ssn":"333-33-3333"}' >/dev/null
echo "  created smoke_claims (2 docs) and smoke_pii (1 doc)"

echo "== Case 1: catalog grant with a deny below it =="
# What the translator emits once a deny of smoke_pii sits under an allow of "*": the pattern is
# replaced by the indices that remain allowed.
admin PUT "/_security/role/${ROLE_PREFIX}reader" \
  '{"cluster":[],"indices":[{"names":["smoke_claims"],"privileges":["read","view_index_metadata"]}]}' >/dev/null
admin PUT "/_security/user/$TEST_USER" \
  "{\"password\":\"$TEST_PASSWORD\",\"roles\":[\"${ROLE_PREFIX}reader\"]}" >/dev/null

if as_test_user GET /smoke_claims/_search | grep -q '"hits"'; then
  pass "allowed index is readable"
else
  fail "allowed index is readable"
fi
if as_test_user GET /smoke_pii/_search | grep -q 'security_exception\|index_not_found'; then
  pass "denied index is not readable"
else
  fail "denied index is not readable"
fi

if [[ "$PAID" == "1" ]]; then
  echo "== Case 2: column restriction becomes field_security.except =="
  admin PUT "/_security/role/${ROLE_PREFIX}reader" \
    '{"cluster":[],"indices":[{"names":["smoke_claims"],"privileges":["read","view_index_metadata"],"field_security":{"grant":["*"],"except":["ssn"]}}]}' >/dev/null
  RESULT=$(as_test_user GET /smoke_claims/_search)
  if echo "$RESULT" | grep -q '"amount"' && ! echo "$RESULT" | grep -q '"ssn"'; then
    pass "restricted field is absent from _source"
  else
    fail "restricted field is absent from _source"
  fi
  RESULT=$(as_test_user POST /smoke_claims/_search '{"query":{"term":{"ssn":"111-11-1111"}}}')
  if echo "$RESULT" | grep -q '"value" *: *0\|"value":0'; then
    pass "restricted field cannot be queried"
  else
    fail "restricted field cannot be queried"
  fi

  echo "== Case 3: row filter becomes a document-level security query =="
  admin PUT "/_security/role/${ROLE_PREFIX}reader" \
    '{"cluster":[],"indices":[{"names":["smoke_claims"],"privileges":["read","view_index_metadata"],"query":{"term":{"region":"west"}}}]}' >/dev/null
  RESULT=$(as_test_user GET /smoke_claims/_search)
  if echo "$RESULT" | grep -q '"west"' && ! echo "$RESULT" | grep -q '"east"'; then
    pass "only documents matching the filter are returned"
  else
    fail "only documents matching the filter are returned"
  fi
  RESULT=$(as_test_user POST '/smoke_claims/_search' '{"size":0,"aggs":{"total":{"sum":{"field":"amount"}}}}')
  if echo "$RESULT" | grep -q '100\.0\|100'; then
    pass "aggregations respect the filter"
  else
    fail "aggregations respect the filter"
  fi

  echo "== Case 4: roles union, so a second unrestricted role defeats the filter =="
  admin PUT "/_security/role/${ROLE_PREFIX}unrestricted" \
    '{"cluster":[],"indices":[{"names":["smoke_claims"],"privileges":["read"]}]}' >/dev/null
  admin PUT "/_security/user/$TEST_USER" \
    "{\"password\":\"$TEST_PASSWORD\",\"roles\":[\"${ROLE_PREFIX}reader\",\"${ROLE_PREFIX}unrestricted\"]}" >/dev/null
  RESULT=$(as_test_user GET /smoke_claims/_search)
  if echo "$RESULT" | grep -q '"east"'; then
    pass "confirmed: an unrestricted role beside a restricted one removes the filter"
  else
    fail "expected the union to remove the filter, but it held"
  fi
fi

echo "== Cleanup =="
admin DELETE "/_security/user/$TEST_USER" >/dev/null 2>&1 || true
admin DELETE "/_security/role/${ROLE_PREFIX}reader" >/dev/null 2>&1 || true
admin DELETE "/_security/role/${ROLE_PREFIX}unrestricted" >/dev/null 2>&1 || true
admin DELETE "/smoke_claims,/smoke_pii" >/dev/null 2>&1 || true
echo "  removed test users, roles and indices"

if [[ "$FAILURES" -gt 0 ]]; then
  echo "$FAILURES check(s) failed"
  exit 1
fi
echo "All checks passed"
