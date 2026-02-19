#!/usr/bin/env bash
set -euo pipefail
CERT_DIR="$(cd "$(dirname "$0")" && pwd)/mosquitto/certs"
mkdir -p "$CERT_DIR"
openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes   -subj "/C=IT/O=CCI/CN=cci-ca"   -keyout "$CERT_DIR/ca.key" -out "$CERT_DIR/ca.crt"
openssl req -new -newkey rsa:2048 -nodes   -subj "/C=IT/O=CCI/CN=mosquitto"   -keyout "$CERT_DIR/server.key" -out "$CERT_DIR/server.csr"
openssl x509 -req -in "$CERT_DIR/server.csr" -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial   -out "$CERT_DIR/server.crt" -days 825 -sha256
rm -f "$CERT_DIR/server.csr" "$CERT_DIR/ca.srl"
chmod 600 "$CERT_DIR/server.key"
echo "Done. Copy mosquitto/certs/ca.crt to every Edge device: edge/certs/ca.crt"
