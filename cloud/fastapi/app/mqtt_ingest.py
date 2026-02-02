import json, os, threading, time
from typing import Optional

import paho.mqtt.client as mqtt
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from app.influx import get_influx_client

_thread: Optional[threading.Thread] = None


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def write_edge_status(influx, site_id: str, payload: dict):
    # writes edge connectivity status to influx measurement 'edge_status'
    p = Point("edge_status").tag("site_id", site_id)

    if "mqtt_connected" in payload:
        p = p.field("mqtt_connected", bool(payload.get("mqtt_connected")))
    if "edge_id" in payload:
        p = p.field("edge_id", str(payload.get("edge_id")))
    if "hostname" in payload:
        p = p.field("hostname", str(payload.get("hostname")))

    # optional: store target count for UI
    targets = payload.get("targets")
    if isinstance(targets, list):
        p = p.field("targets_n", int(len(targets)))

    ts = payload.get("ts")
    if isinstance(ts, int):
        p = p.time(ts * 1_000_000_000, WritePrecision.NS)

    influx.write_api().write(
        bucket=os.getenv("INFLUX_BUCKET", ""),
        org=os.getenv("INFLUX_ORG", ""),
        record=p,
        write_precision=WritePrecision.S,
    )


def write_modbus_event(influx, measurement: str, site_id: str, payload: dict):
    # measurement: "modbus_status" or "modbus_error"
    p = Point(measurement).tag("site_id", site_id)

    # tags-ish fields
    for k in ("device", "host", "port", "unit_id"):
        if k in payload and payload[k] is not None:
            p = p.tag(str(k), str(payload[k]))

    # fields
    if "modbus_ok" in payload:
        p = p.field("modbus_ok", bool(payload.get("modbus_ok")))
    if "error" in payload:
        p = p.field("error", str(payload.get("error")))

    # optional: results summary as string (avoid high-cardinality tags)
    if "results" in payload:
        try:
            p = p.field("results_json", json.dumps(payload.get("results")))
        except Exception:
            pass

    ts = payload.get("ts")
    if isinstance(ts, int):
        p = p.time(ts * 1_000_000_000, WritePrecision.NS)

    influx.write_api().write(
        bucket=os.getenv("INFLUX_BUCKET", ""),
        org=os.getenv("INFLUX_ORG", ""),
        record=p,
        write_precision=WritePrecision.S,
    )


def _on_connect(client, userdata, flags, reason_code, properties=None):
    # subscribe to telemetry + status + modbus topics
    base = os.getenv("MQTT_TOPIC_BASE", "pv")

    # legacy / default telemetry
    client.subscribe(os.getenv("MQTT_SUB_TOPIC", f"{base}/+/telemetry"), qos=1)

    # new topics
    client.subscribe(f"{base}/+/status", qos=1)
    client.subscribe(f"{base}/+/modbus_status", qos=1)
    client.subscribe(f"{base}/+/modbus_error", qos=1)


def _on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception:
        return
    if not isinstance(payload, dict):
        return

    topic = msg.topic or ""
    site_id = payload.get("site_id")
    if not site_id:
        return

    influx = userdata["influx"]

    # 1) edge connectivity status
    if topic.endswith("/status"):
        try:
            write_edge_status(influx, site_id, payload)
        except Exception:
            pass
        return

    # 2) modbus status/error events
    if topic.endswith("/modbus_status"):
        try:
            write_modbus_event(influx, "modbus_status", site_id, payload)
        except Exception:
            pass
        return

    if topic.endswith("/modbus_error"):
        try:
            write_modbus_event(influx, "modbus_error", site_id, payload)
        except Exception:
            pass
        return

    # 3) telemetry ingest (existing behavior)
    meas = payload.get("measurement", "pv_telemetry")
    fields = payload.get("fields", {})
    tags = payload.get("tags", {})
    if not isinstance(fields, dict):
        return

    p = Point(meas).tag("site_id", site_id)
    for k, v in (tags or {}).items():
        if v is not None:
            p = p.tag(str(k), str(v))
    for k, v in fields.items():
        if v is None:
            continue
        p = p.field(str(k), v)

    userdata["write_api"].write(
        bucket=os.getenv("INFLUX_BUCKET", ""),
        org=os.getenv("INFLUX_ORG", ""),
        record=p,
        write_precision=WritePrecision.S,
    )


def _run():
    influx = get_influx_client()
    write_api = influx.write_api(write_options=SYNCHRONOUS)

    host = os.getenv("MQTT_HOST", "mosquitto")
    port = int(os.getenv("MQTT_PORT", "8883"))
    user = os.getenv("MQTT_USERNAME", "cr_ingest")
    pw = os.getenv("MQTT_PASSWORD", "")
    ca = os.getenv("MQTT_TLS_CA", "/mosquitto/certs/ca.crt")
    insecure = _env_bool("MQTT_TLS_INSECURE", False)

    client = mqtt.Client(protocol=mqtt.MQTTv5)
    client.username_pw_set(user, pw)
    client.tls_set(ca_certs=ca)
    client.tls_insecure_set(insecure)
    client.on_connect = _on_connect
    client.on_message = _on_message
    client.user_data_set({"write_api": write_api, "influx": influx})

    while True:
        try:
            client.connect(host, port, keepalive=30)
            client.loop_forever(retry_first_connection=True)
        except Exception:
            time.sleep(2)


def start_ingest():
    global _thread
    if _thread and _thread.is_alive():
        return
    _thread = threading.Thread(target=_run, daemon=True)
    _thread.start()
