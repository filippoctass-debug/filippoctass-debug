import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

import paho.mqtt.client as mqtt
from influxdb_client import Point, WritePrecision

from app.influx import get_influx_client

_thread: Optional[threading.Thread] = None

# -------------------------
# Small helpers / logging
# -------------------------
def _log(msg: str):
    print(f"[mqtt_ingest] {msg}", flush=True)

def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def _safe_json(payload) -> str:
    try:
        return json.dumps(payload)[:1200]
    except Exception:
        return "<unserializable>"

def _parse_ts_to_dt(ts_val) -> Optional[datetime]:
    """
    Supporta:
      - ts int (epoch seconds)
      - ts str ISO8601 "2026-02-20T08:30:24Z"
    """
    if ts_val is None:
        return None
    if isinstance(ts_val, int):
        try:
            return datetime.fromtimestamp(ts_val, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(ts_val, float):
        try:
            return datetime.fromtimestamp(int(ts_val), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(ts_val, str):
        s = ts_val.strip()
        if not s:
            return None
        try:
            # support "Z"
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            return datetime.fromisoformat(s).astimezone(timezone.utc)
        except Exception:
            return None
    return None

def _apply_time(p: Point, payload: dict) -> Point:
    dt = _parse_ts_to_dt(payload.get("ts"))
    if dt is not None:
        # precision seconds per coerenza col resto
        return p.time(dt, WritePrecision.S)
    return p

# -------------------------
# Cache targets from /status
# -------------------------
# site_id -> device_name -> (host, port, unit_id)
_TARGETS_LOCK = threading.Lock()
_TARGETS_BY_SITE: Dict[str, Dict[str, Tuple[str, int, int]]] = {}

def _update_targets_cache(site_id: str, payload: dict) -> None:
    """
    Legge payload["targets"] e salva mapping name -> (host,port,unit_id).
    Esempio targets:
      [{"name":"dev1","host":"192.168.2.108","port":5020,"unit_id":1}, ...]
    """
    targets = payload.get("targets")
    if not isinstance(targets, list):
        return

    mapping: Dict[str, Tuple[str, int, int]] = {}
    for t in targets:
        if not isinstance(t, dict):
            continue
        name = str(t.get("name") or "").strip()
        host = str(t.get("host") or "").strip()
        port = t.get("port")
        unit = t.get("unit_id")
        if not name or not host:
            continue
        try:
            port_i = int(port) if port is not None else 502
            unit_i = int(unit) if unit is not None else 1
        except Exception:
            continue
        mapping[name] = (host, port_i, unit_i)

    if not mapping:
        return

    with _TARGETS_LOCK:
        _TARGETS_BY_SITE[str(site_id)] = mapping

def _lookup_target(site_id: str, device_name: str) -> Optional[Tuple[str, int, int]]:
    with _TARGETS_LOCK:
        m = _TARGETS_BY_SITE.get(str(site_id)) or {}
        return m.get(str(device_name))

# -------------------------
# MQTT callbacks
# -------------------------
def _on_connect(client, userdata, flags, reason_code, properties=None):
    base = os.getenv("MQTT_TOPIC_BASE", "pv")

    telemetry = os.getenv("MQTT_SUB_TOPIC", f"{base}/+/telemetry")
    status = f"{base}/+/status"
    modbus_status = f"{base}/+/modbus_status"
    modbus_error = f"{base}/+/modbus_error"

    # DEBUG: listen commands too (to verify if backend publishes)
    cmd = f"{base}/+/cmd"

    _log(
        f"CONNECTED rc={reason_code}. subscribing to: "
        f"{telemetry}, {status}, {modbus_status}, {modbus_error}, {cmd}"
    )

    client.subscribe(telemetry, qos=1)
    client.subscribe(status, qos=1)
    client.subscribe(modbus_status, qos=1)
    client.subscribe(modbus_error, qos=1)
    client.subscribe(cmd, qos=1)

# -------------------------
# Influx writers
# -------------------------
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

    p = _apply_time(p, payload)

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

    p = _apply_time(p, payload)

    influx.write_api().write(
        bucket=os.getenv("INFLUX_BUCKET", ""),
        org=os.getenv("INFLUX_ORG", ""),
        record=p,
        write_precision=WritePrecision.S,
    )

def _write_modbus_point(
    influx,
    measurement: str,
    site_id: str,
    payload: dict,
    *,
    device_name: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    unit_id: Optional[int] = None,
    ok_val: Optional[bool] = None,
    err_str: Optional[str] = None,
    results_json: Optional[str] = None,
):
    """
    Un punto modbus_status/modbus_error, con tag utili alla dashboard:
      - site_id (tag)
      - device (tag)  -> IMPORTANT per devices_api (TAG_DEVICE default "device")
      - host/port/unit_id (tag) -> IMPORTANT per /devices/status che indicizza per unit_id
    """
    p = Point(measurement).tag("site_id", str(site_id))

    if device_name:
        p = p.tag("device", str(device_name))
    if host:
        p = p.tag("host", str(host))
    if port is not None:
        p = p.tag("port", str(port))
    if unit_id is not None:
        p = p.tag("unit_id", str(unit_id))

    # fields
    if ok_val is not None:
        p = p.field("modbus_ok", bool(ok_val))
    if err_str:
        p = p.field("error", str(err_str))
    if results_json:
        p = p.field("results_json", results_json)

    p = _apply_time(p, payload)

    influx.write_api().write(
        bucket=os.getenv("INFLUX_BUCKET", ""),
        org=os.getenv("INFLUX_ORG", ""),
        record=p,
        write_precision=WritePrecision.S,
    )

def _write_modbus_events(influx, measurement: str, site_id: str, payload: dict):
    """
    Supporta due formati:
      A) payload già "flat" con device/host/port/unit_id/modbus_ok
      B) payload con results=[{name, ok}, ...]
         -> scrive un punto PER device, arricchendo con host/port/unit_id dalla cache /status.
    """
    # caso B: results list
    results = payload.get("results")
    if isinstance(results, list) and results:
        # salva anche results_json sul punto aggregato (senza device)
        try:
            rjson = json.dumps(results)
        except Exception:
            rjson = None

        # punto aggregato (senza device), utile come "heartbeat"
        agg_ok = payload.get("modbus_ok")
        agg_err = payload.get("error")
        _write_modbus_point(
            influx,
            measurement,
            site_id,
            payload,
            ok_val=bool(agg_ok) if agg_ok is not None else None,
            err_str=str(agg_err) if agg_err is not None else None,
            results_json=rjson,
        )

        # punti per singolo device
        for r in results:
            if not isinstance(r, dict):
                continue
            name = str(r.get("name") or r.get("device") or "").strip()
            if not name:
                continue
            ok = r.get("ok")
            ok_b = bool(ok) if isinstance(ok, (bool, int, float)) else None

            host = port = unit = None
            tinfo = _lookup_target(site_id, name)
            if tinfo:
                host, port, unit = tinfo

            _write_modbus_point(
                influx,
                measurement,
                site_id,
                payload,
                device_name=name,
                host=host,
                port=port,
                unit_id=unit,
                ok_val=ok_b,
                err_str=None,
                results_json=None,
            )
        return

    # caso A: flat
    device = payload.get("device")
    host = payload.get("host")
    port = payload.get("port")
    unit = payload.get("unit_id")

    ok_val = payload.get("modbus_ok")
    err_str = payload.get("error")

    try:
        port_i = int(port) if port is not None else None
    except Exception:
        port_i = None
    try:
        unit_i = int(unit) if unit is not None else None
    except Exception:
        unit_i = None

    _write_modbus_point(
        influx,
        measurement,
        site_id,
        payload,
        device_name=str(device) if device is not None else None,
        host=str(host) if host is not None else None,
        port=port_i,
        unit_id=unit_i,
        ok_val=bool(ok_val) if ok_val is not None else None,
        err_str=str(err_str) if err_str is not None else None,
    )

# -------------------------
# on_message router
# -------------------------
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

    # debug cmd topics
    if topic.endswith("/cmd"):
        _log(f"CMD RX topic={topic} payload={_safe_json(payload)}")
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
            # IMPORTANT: cache targets for later modbus_status expansion
            _update_targets_cache(site_id, payload)

            _write_edge_status(influx, site_id, payload)
            _log(f"WROTE edge_status site_id={site_id}")
            return

        if topic.endswith("/modbus_status"):
            _write_modbus_events(influx, "modbus_status", site_id, payload)
            _log(f"WROTE modbus_status site_id={site_id}")
            return

        if topic.endswith("/modbus_error"):
            _write_modbus_events(influx, "modbus_error", site_id, payload)
            _log(f"WROTE modbus_error site_id={site_id}")
            return

        _write_telemetry(influx, site_id, payload)
        _log(
            f"WROTE telemetry site_id={site_id} "
            f"meas={payload.get('measurement','pv_telemetry')}"
        )
    except Exception as e:
        _log(f"ERROR write topic={topic} site_id={site_id} err={e}")

# -------------------------
# thread runner
# -------------------------
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
