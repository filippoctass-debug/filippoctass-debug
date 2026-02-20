from __future__ import annotations

import json
import os
import ssl
import time
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt


def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


def mqtt_publish(topic: str, payload: Dict[str, Any], *, retain: bool = True, qos: int = 1) -> None:
    host = _env("MQTT_HOST", "mosquitto")
    port = int(_env("MQTT_PORT", "8883"))
    user = _env("MQTT_USER", "")
    password = _env("MQTT_PASS", "")
    cafile = _env("MQTT_CA", "/mosquitto/certs/ca.crt")
    insecure = _env("MQTT_INSECURE", "false").lower() in ("1", "true", "yes", "y")

    data = json.dumps(payload, ensure_ascii=False)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"cr_api_pub_{int(time.time()*1000)}")
    if user:
        client.username_pw_set(user, password)

    # TLS
    ctx = ssl.create_default_context(cafile=cafile if cafile else None)
    if insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    client.tls_set_context(ctx)
    client.tls_insecure_set(insecure)

    # connect/publish/disconnect (semplice e robusto)
    client.connect(host, port, keepalive=20)
    client.loop_start()
    try:
        info = client.publish(topic, data.encode("utf-8"), qos=qos, retain=retain)
        info.wait_for_publish(timeout=5)
        if not info.is_published():
            raise RuntimeError("publish not completed")
    finally:
        client.loop_stop()
        client.disconnect()


def publish_targets_cmd(site_id: str, targets: list[dict[str, Any]]) -> None:
    topic = f"pv/{site_id}/cmd"
    payload = {
        "site_id": site_id,
        "cmd": "set_targets",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "targets": targets,
    }
    mqtt_publish(topic, payload, retain=True, qos=1)
