#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
CERT_DIR="$SCRIPT_DIR/.generated/certs"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

cleanup() {
  if [[ "${LDAP_IT_KEEP:-0}" == "1" ]]; then
    echo "LDAP fixture left running because LDAP_IT_KEEP=1"
    return
  fi
  docker compose -f "$COMPOSE_FILE" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

command -v docker >/dev/null 2>&1 || {
  echo "docker is required" >&2
  exit 1
}
command -v openssl >/dev/null 2>&1 || {
  echo "openssl is required to generate the ephemeral test CA" >&2
  exit 1
}
docker compose version >/dev/null

mkdir -p "$CERT_DIR"
rm -f "$CERT_DIR/ca.crt" "$CERT_DIR/ca.key" "$CERT_DIR/ca.srl" \
  "$CERT_DIR/server.crt" "$CERT_DIR/server.csr" "$CERT_DIR/server.key"

openssl req -x509 -newkey rsa:2048 -sha256 -days 2 -nodes \
  -subj '/CN=WeKnora LDAP Integration Test CA' \
  -keyout "$CERT_DIR/ca.key" -out "$CERT_DIR/ca.crt" >/dev/null 2>&1
openssl req -newkey rsa:2048 -sha256 -nodes \
  -subj '/CN=openldap' \
  -keyout "$CERT_DIR/server.key" -out "$CERT_DIR/server.csr" >/dev/null 2>&1
openssl x509 -req -sha256 -days 2 \
  -in "$CERT_DIR/server.csr" \
  -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" \
  -CAserial "$CERT_DIR/ca.srl" -CAcreateserial \
  -extfile "$SCRIPT_DIR/certs/server-ext.cnf" \
  -out "$CERT_DIR/server.crt" >/dev/null 2>&1
chmod 0644 "$CERT_DIR/ca.crt" "$CERT_DIR/server.crt" "$CERT_DIR/server.key"

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-weknora-ldap-integration}"

docker compose -f "$COMPOSE_FILE" up -d --wait openldap
docker compose -f "$COMPOSE_FILE" run --rm test
