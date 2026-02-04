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

def _log(msg: str):
    # log semplice su stdout (docker logs)
    print(f"[mqtt_ingest] {msg}", flush=True)

def write_edge_status(influx, site_id: str, payload: dict):
    p = Point("edge_status").tag("site_id", site_id)

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
        p = p.time(ts * 1_000_000_000, WritePrecision.NS)

    influx.write_api().write(
        bucket=os.getenv("INFLUX_BUCKET", ""),
        org=os.getenv("INFLUX_ORG", ""),
        record=p,
        write_precision=WritePrecision.S,
    )

def write_modbus_event(influx, measurement: str, site_id: str, payload: dict):
    p = Point(measurement).tag("site_id", site_id)

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
        p = p.time(ts * 1_000_000_000, WritePrecision.NS)

    influx.write_api().write(
        bucket=os.getenv("INFLUX_BUCKET", ""),
        org=os.getenv("INFLUX_ORG", ""),
        record=p,
        write_precision=WritePrecision.S,
    )

def _on_connect(client, userdata, flags, reason_code, properties=None):
    base = os.getenv("MQTT_TOPIC_BASE", "pv")
    _log(f"CONNECTED reason_code={reason_code} host={os.getenv('MQTT_HOST')} port={os.getenv('MQTT_PORT')} insecure={os.getenv('MQTT_TLS_INSECURE')}")
    topics = [
        os.getenv("MQTT_SUB_TOPIC", f"{base}/+/telemetry"),
        f"{base}/+/status",
        f"{base}/+/modbus_status",
        f"{base}/+/modbus_error",
    ]
    for t in topics:
        _log(f"SUBSCRIBE {t}")
        client.subscribe(t, qos=1)

def _on_message(client, userdata, msg):
    topic = msg.topic or ""
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except Exception as e:
        _log(f"DROP non-json topic={topic} err={e}")
        return
    if not isinstance(payload, dict):
        _log(f"DROP non-dict topic={topic}")
        return

    site_id = payload.get("site_id")
    if not site_id:
        _log(f"DROP no site_id topic={topic} keys={list(payload.keys())}")
        return

    influx = userdata["influx"]

    try:
        # 1) edge status
        if topic.endswith("/status"):
            write_edge_status(influx, site_id, payload)
            _log(f"WROTE edge_status site_id={site_id}")
            return

        # 2) modbus status/error
        if topic.endswith("/modbus_status"):
            write_modbus_event(influx, "modbus_status", site_id, payload)
            _log(f"WROTE modbus_status site_id={site_id}")
            return

        if topic.endswith("/modbus_error"):
            write_modbus_event(influx, "modbus_error", site_id, payload)
            _log(f"WROTE modbus_error site_id={site_id}")
            return

        # 3) telemetry
        meas = payload.get("measurement", "pv_telemetry")
        fields = payload.get("fields", {})
        tags = payload.get("tags", {})
        if not isinstance(fields, dict):
            _log(f"DROP bad fields topic={topic} site_id={site_id}")
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
        _log(f"WROTE telemetry meas={meas} site_id={site_id} fields={list(fields.keys())}")

    except Exception as e:
        _log(f"ERROR write topic={topic} site_id={site_id} err={e}")

def _run():
    influx = get_influx_client()
    write_api = influx.write_api(write_options=SYNCHRONOUS)

    host = os.getenv("MQTT_HOST", "mosquitto")
    port = int(os.getenv("MQTT_PORT", "8883"))
    user = os.getenv("MQTT_USERNAME", "cr_ingest")
    pw = os.getenv("MQTT_PASSWORD", "")
    ca = os.getenv("MQTT_TLS_CA", "/mosquitto/certs/ca.crt")
    insecure = _env_bool("MQTT_TLS_INSECURE", False)

    _log(f"START host={host} port={port} user={user} ca={ca} insecure={insecure} bucket={os.getenv('INFLUX_BUCKET')} org={os.getenv('INFLUX_ORG')} url={os.getenv('INFLUX_URL')}")

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
        except Exception as e:
            _log(f"CONNECT ERROR err={e} (retry in 2s)")
            time.sleep(2)

def start_ingest():
    global _thread
    if _thread and _thread.is_alive():
        return
    _thread = threading.Thread(target=_run, daemon=True)
    _thread.start()
