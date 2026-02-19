#!/usr/bin/env bash
set -euo pipefail
SITE_ID="${1:-}"
if [[ -z "$SITE_ID" ]]; then echo "Usage: $0 <SITE_ID> [--cloud-host <host>]"; exit 1; fi
CLOUD_HOST="host.docker.internal"
if [[ "${2:-}" == "--cloud-host" ]]; then CLOUD_HOST="${3:-host.docker.internal}"; fi
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUNDLES_DIR="$ROOT_DIR/bundles"
mkdir -p "$BUNDLES_DIR"
USER="edge_${SITE_ID,,}"; USER="${USER//-/_}"
PASS="$(python3 - <<'PY'
import secrets,string
alphabet=string.ascii_letters+string.digits+"!%_-"
print("".join(secrets.choice(alphabet) for _ in range(24)))
PY
)"
docker run --rm -i -v "$ROOT_DIR/mosquitto/config:/mosquitto/config" eclipse-mosquitto:2.0   mosquitto_passwd -b /mosquitto/config/passwords "$USER" "$PASS" >/dev/null
ACL="$ROOT_DIR/mosquitto/config/aclfile"
if ! grep -q "^user $USER$" "$ACL"; then
  { echo ""; echo "# $SITE_ID"; echo "user $USER"; echo "topic write pv/$SITE_ID/#"; echo "topic read pv/$SITE_ID/#"; } >> "$ACL"
fi
docker compose restart mosquitto >/dev/null
BUNDLE_DIR="$BUNDLES_DIR/${SITE_ID}_edge_bundle"
rm -rf "$BUNDLE_DIR"; mkdir -p "$BUNDLE_DIR/certs"
cp "$ROOT_DIR/mosquitto/certs/ca.crt" "$BUNDLE_DIR/certs/ca.crt"
cat > "$BUNDLE_DIR/.env" <<EOF
SITE_ID=$SITE_ID
MODBUS_HOST=192.168.1.10
MODBUS_PORT=502
MQTT_HOST=$CLOUD_HOST
MQTT_PORT=8883
MQTT_USERNAME=$USER
MQTT_PASSWORD=$PASS
MQTT_TLS_CA=/app/certs/ca.crt
MQTT_TLS_INSECURE=false
MQTT_TOPIC_BASE=pv
PUBLISH_INTERVAL_S=5
EOF
ZIP_PATH="$BUNDLES_DIR/${SITE_ID}_edge_bundle.zip"
python3 - <<PY
import os, zipfile
bundle_dir=r"$BUNDLE_DIR"; zip_path=r"$ZIP_PATH"; site=r"$SITE_ID"
with zipfile.ZipFile(zip_path,"w",zipfile.ZIP_DEFLATED) as z:
  for root,_,files in os.walk(bundle_dir):
    for fn in files:
      p=os.path.join(root,fn)
      arc=os.path.relpath(p, bundle_dir)
      z.write(p, arcname=os.path.join(f"{site}_edge_bundle", arc))
print(zip_path)
PY
echo "Bundle created: $ZIP_PATH"
echo "MQTT_USERNAME=$USER"
echo "MQTT_PASSWORD=$PASS"
