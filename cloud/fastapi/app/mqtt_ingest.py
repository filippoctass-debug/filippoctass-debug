import json
import os
import threading
import time
from typing import Optional

import paho.mqtt.client as mqtt
from influxdb_client import Point, WritePrecision

from app.influx import get_influx_client

_thread: Optional[threading.Thread] = None


def _log(msg: str):
    print(f"[mqtt_ingest] {msg}", flush=True)


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _safe_json(payload) -> str:
    try:
        return json.dumps(payload)[:1000]
    except Exception:
        return "<unserializable>"


def _on_connect(client, userdata, flags, reason_code, properties=None):
    base = os.getenv("MQTT_TOPIC_BASE", "pv")

    telemetry = os.getenv("MQTT_SUB_TOPIC", f"{base}/+/telemetry")
    status = f"{base}/+/status"
    modbus_status = f"{base}/+/modbus_status"
    modbus_error = f"{base}/+/modbus_error"

    _log(
        f"CONNECTED rc={reason_code}. subscribing to: "
        f"{telemetry}, {status}, {modbus_status}, {modbus_error}"
    )

    client.subscribe(telemetry, qos=1)
    client.subscribe(status, qos=1)
    client.subscribe(modbus_status, qos=1)
    client.subscribe(modbus_error, qos=1)


def _write_telemetry(influx, site_id: str, payload: dict):
    meas = payload.get("measurement", "pv_telemetry")
    fields = payload.get("fields", {})
    tags = payload.get("tags", {})

    if not isinstance(fields, dict) or not fields:
        return

    p = Point(meas).tag("site_id", str(site_id))

    if isinstance(tags, dict):
        for k, v in tags.items():
            if v is not None:
                p = p.tag(str(k), str(v))

    for k, v in fields.items():
        if v is None:
            continue
        p = p.field(str(k), v)

    ts = payload.get("ts")
    if isinstance(ts, int):
        p = p.time(ts, WritePrecision.S)

    influx.write_api().write(
        bucket=os.getenv("INFLUX_BUCKET", ""),
        org=os.getenv("INFLUX_ORG", ""),
        record=p,
        write_precision=WritePrecision.S,
    )


def _write_edge_status(influx, site_id: str, payload: dict):
    p = Point("edge_status").tag("site_id", str(site_id))

    if "mqtt_connected" in payload:
        p = p.field("mqtt_connected", bool(payload.get("mqtt_connected")))
    if "edge_id" in payload:
        p = p.field("edge_id", str(payload.get("edge_id")))
    if "hostname" in payload:
        p = p.field("hostname", str(payload.get("hostname")))

    targets = payload.get("targets")
    if isinstance(targets, list):
        p = p.field("targets_n", int(len(targets)))

    ts = payload.get("ts")
    if isinstance(ts, int):
        p = p.time(ts, WritePrecision.S)

    influx.write_api().write(
        bucket=os.getenv("INFLUX_BUCKET", ""),
        org=os.getenv("INFLUX_ORG", ""),
        record=p,
        write_precision=WritePrecision.S,
    )


def _write_modbus_event(influx, measurement: str, site_id: str, payload: dict):
    p = Point(measurement).tag("site_id", str(site_id))

    for k in ("device", "host", "port", "unit_id"):
        if k in payload and payload[k] is not None:
            p = p.tag(str(k), str(payload[k]))

    if "modbus_ok" in payload:
        p = p.field("modbus_ok", bool(payload.get("modbus_ok")))
    if "error" in payload:
        p = p.field("error", str(payload.get("error")))

    if "results" in payload:
        try:
            p = p.field("results_json", json.dumps(payload.get("results")))
        except Exception:
            pass

    ts = payload.get("ts")
    if isinstance(ts, int):
        p = p.time(ts, WritePrecision.S)

    influx.write_api().write(
        bucket=os.getenv("INFLUX_BUCKET", ""),
        org=os.getenv("INFLUX_ORG", ""),
        record=p,
        write_precision=WritePrecision.S,
    )


def _on_message(client, userdata, msg):
    topic = msg.topic or ""
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception as e:
        _log(f"DROP non-json topic={topic} err={e}")
        return

    if not isinstance(payload, dict):
        _log(f"DROP non-dict topic={topic} payload={type(payload)}")
        return

    site_id = payload.get("site_id")
    if not site_id:
        _log(f"DROP missing site_id topic={topic} payload={_safe_json(payload)}")
        return


    # persist site in registry (once seen, stays until user deletes)
    try:
        from app.sites import register_site
        register_site(site_id)
    except Exception as e:
        _log(f"registry WARN site_id={site_id} err={e}")
    influx = userdata["influx"]

    try:
        if topic.endswith("/status"):
            _write_edge_status(influx, site_id, payload)
            _log(f"WROTE edge_status site_id={site_id}")
            return

        if topic.endswith("/modbus_status"):
            _write_modbus_event(influx, "modbus_status", site_id, payload)
            _log(f"WROTE modbus_status site_id={site_id}")
            return

        if topic.endswith("/modbus_error"):
            _write_modbus_event(influx, "modbus_error", site_id, payload)
            _log(f"WROTE modbus_error site_id={site_id}")
            return

        _write_telemetry(influx, site_id, payload)
        _log(
            f"WROTE telemetry site_id={site_id} "
            f"meas={payload.get('measurement','pv_telemetry')}"
        )
    except Exception as e:
        _log(f"ERROR write topic={topic} site_id={site_id} err={e}")


def _run():
    _log("START mqtt_ingest thread")

    influx = get_influx_client()
    try:
        ok = influx.ping()
        _log(f"INFLUX ping={ok} url={os.getenv('INFLUX_URL','')}")
    except Exception as e:
        _log(f"INFLUX ping ERROR: {e}")

    host = os.getenv("MQTT_HOST", "mosquitto")
    port = int(os.getenv("MQTT_PORT", "8883"))
    user = os.getenv("MQTT_USERNAME", "cr_ingest")
    pw = os.getenv("MQTT_PASSWORD", "")
    ca = os.getenv("MQTT_TLS_CA", "/mosquitto/certs/ca.crt")
    insecure = _env_bool("MQTT_TLS_INSECURE", False)

    _log(f"MQTT cfg host={host} port={port} user={user} ca={ca} insecure={insecure}")

    client = mqtt.Client(protocol=mqtt.MQTTv5)
    client.username_pw_set(user, pw)

    if insecure:
        # usa trust store di default, ma disabilita verify/hostname (utile in dev)
        client.tls_set()
        client.tls_insecure_set(True)
    else:
        client.tls_set(ca_certs=ca)
        client.tls_insecure_set(False)

    client.on_connect = _on_connect
    client.on_message = _on_message
    client.user_data_set({"influx": influx})

    while True:
        try:
            _log("MQTT connecting...")
            client.connect(host, port, keepalive=30)
            _log("MQTT loop_forever()")
            client.loop_forever(retry_first_connection=True)
        except Exception as e:
            _log(f"MQTT loop ERROR: {e}. retry in 2s")
            time.sleep(2)


def start_ingest():
    global _thread
    if _thread and _thread.is_alive():
        return
    _thread = threading.Thread(target=_run, daemon=True)
    _thread.start()

