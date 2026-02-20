from __future__ import annotations

import json
import os
import tempfile
import threading
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

import paho.mqtt.client as mqtt

# Influx (allineato al resto del progetto: pv_aggregate_api.py usa app.influx.get_influx_client)
from influxdb_client import InfluxDBClient
from app.influx import get_influx_client

router = APIRouter(tags=["devices"])

_LOCK = threading.Lock()

# ---- Influx env allineati a pv_aggregate_api.py ----
INFLUX_ORG = os.getenv("INFLUX_ORG", os.getenv("INFLUXD_INIT_ORG", "pv"))
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", os.getenv("INFLUXD_INIT_BUCKET", "cci"))
MEASUREMENT = os.getenv("INFLUX_MEASUREMENT_TELEMETRY", "pv_telemetry_v2")
TAG_SITE = os.getenv("INFLUX_TAG_SITE", "site_id")
TAG_DEVICE = os.getenv("INFLUX_TAG_DEVICE_ID", "device")  # IMPORTANT: default "device"


def _bm_dump(m):
    # compat pydantic v1/v2
    if hasattr(m, "model_dump"):
        return m.model_dump()
    if hasattr(m, "dict"):
        return m.dict()
    return dict(m)


def _storage_path() -> Path:
    # /app/app/storage in container, in repo è cloud/fastapi/app/storage
    here = Path(__file__).resolve().parent
    return here / "storage" / "devices_registry.json"


def _read_registry() -> Dict[str, Any]:
    p = _storage_path()
    if not p.exists():
        return {"version": 1, "sites": {}}
    try:
        raw = p.read_text(encoding="utf-8", errors="ignore").strip()
        if not raw:
            return {"version": 1, "sites": {}}
        data = json.loads(raw)
        if not isinstance(data, dict):
            return {"version": 1, "sites": {}}
        data.setdefault("version", 1)
        data.setdefault("sites", {})
        if not isinstance(data["sites"], dict):
            data["sites"] = {}
        return data
    except Exception:
        # non facciamo crashare tutto per un json sporco
        return {"version": 1, "sites": {}}


def _atomic_write(p: Path, data: Dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, indent=2, ensure_ascii=False) + "\n"

    # IMPORTANT: temp file MUST be created in the SAME directory as target file
    with tempfile.NamedTemporaryFile(
        "w",
        delete=False,
        encoding="utf-8",
        newline="\n",
        dir=str(p.parent),
        prefix=".tmp_devices_registry_",
        suffix=".json",
    ) as f:
        f.write(payload)
        tmp = f.name

    os.replace(tmp, str(p))


class ModbusDevice(BaseModel):
    # id interno (stringa) che useremo anche per la dashboard
    id: str = Field(..., min_length=1, max_length=64)
    name: str = Field("inverter", max_length=128)

    host: str = Field(..., min_length=1, max_length=255)  # ip/dns
    port: int = Field(502, ge=1, le=65535)
    unit_id: int = Field(1, ge=0, le=247)

    enabled: bool = True
    model: str = Field("sunspec", max_length=32)


class DevicesPayload(BaseModel):
    site_id: str = Field(..., min_length=1, max_length=64)
    devices: List[ModbusDevice] = Field(default_factory=list)


def _normalize_site_id(site_id: str) -> str:
    return str(site_id).strip()


# -----------------------------
# MQTT publish helpers
# -----------------------------
def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _mqtt_publish(topic: str, payload: Dict[str, Any], qos: int = 1, retain: bool = False) -> None:
    """
    Publish sincrono (breve) su MQTT.
    """
    host = os.getenv("MQTT_HOST", "mosquitto")
    port = int(os.getenv("MQTT_PORT", "8883"))
    user = os.getenv("MQTT_USERNAME", "cr_ingest")
    pw = os.getenv("MQTT_PASSWORD", "")
    ca = os.getenv("MQTT_TLS_CA", "/mosquitto/certs/ca.crt")
    insecure = _env_bool("MQTT_TLS_INSECURE", False)

    client = mqtt.Client(protocol=mqtt.MQTTv5)
    if user:
        client.username_pw_set(user, pw)

    if insecure:
        client.tls_set()
        client.tls_insecure_set(True)
    else:
        client.tls_set(ca_certs=ca)
        client.tls_insecure_set(False)

    client.connect(host, port, keepalive=15)
    client.loop_start()
    try:
        data = json.dumps(payload, ensure_ascii=False)
        info = client.publish(topic, payload=data, qos=qos, retain=retain)
        info.wait_for_publish(timeout=5)
    finally:
        try:
            client.loop_stop()
        except Exception:
            pass
        try:
            client.disconnect()
        except Exception:
            pass


def _mqtt_publish_cmd_set_targets(site_id: str, devices_out: List[Dict[str, Any]]) -> None:
    """
    Pubblica pv/{site}/cmd con targets.
    """
    base = os.getenv("MQTT_TOPIC_BASE", "pv")
    topic = f"{base}/{site_id}/cmd"

    targets = []
    for d in devices_out or []:
        if d.get("enabled", True) is False:
            continue
        host = (d.get("host") or "").strip()
        if not host:
            continue
        targets.append(
            {
                "name": (d.get("name") or d.get("id") or "dev").strip(),
                "id": (d.get("id") or "").strip(),
                "host": host,
                "port": int(d.get("port") or 502),
                "unit_id": int(d.get("unit_id") or 1),
                "model": (d.get("model") or "sunspec"),
            }
        )

    payload = {
        "site_id": site_id,
        "cmd": "set_targets",
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "targets": targets,
    }

    _mqtt_publish(topic, payload, qos=1, retain=False)


# -----------------------------
# Registry API
# -----------------------------
@router.get("/site/{site_id}/devices")
def get_devices(site_id: str):
    sid = _normalize_site_id(site_id)
    with _LOCK:
        reg = _read_registry()
        site = reg["sites"].get(sid) or {"devices": []}
        devices = site.get("devices") or []
    return {"site_id": sid, "devices": devices}


@router.put("/site/{site_id}/devices")
def put_devices(site_id: str, payload: DevicesPayload):
    """
    Salva la configurazione devices (slave Modbus) nel registry JSON
    e pubblica un comando MQTT al PV per applicare i targets.
    """
    try:
        sid = _normalize_site_id(site_id)
        if _normalize_site_id(payload.site_id) != sid:
            raise HTTPException(status_code=400, detail="site_id mismatch")

        devices_out: List[Dict[str, Any]] = []
        seen = set()
        for d in payload.devices:
            did = str(d.id).strip()
            if not did:
                raise HTTPException(status_code=400, detail="device id cannot be empty")
            if did in seen:
                raise HTTPException(status_code=400, detail=f"duplicate device id: {did}")
            seen.add(did)
            devices_out.append(_bm_dump(d))

        with _LOCK:
            reg = _read_registry()
            reg.setdefault("version", 1)
            reg.setdefault("sites", {})
            reg["sites"][sid] = {"devices": devices_out}
            _atomic_write(_storage_path(), reg)

        # PUBLISH MQTT (in thread to not block API)
        def _pub():
            try:
                _mqtt_publish_cmd_set_targets(sid, devices_out)
                print(f"[devices_api] MQTT published set_targets site_id={sid} n={len(devices_out)}", flush=True)
            except Exception as e:
                print(f"[devices_api] MQTT publish FAILED site_id={sid} err={e}", flush=True)

        threading.Thread(target=_pub, daemon=True).start()

        return {"site_id": sid, "devices": devices_out, "saved": True, "mqtt_cmd_published": True}

    except HTTPException:
        raise
    except Exception as e:
        print("[devices_api] PUT FAILED:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"put_devices failed: {e}")


# -----------------------------
# Status from Influx
# -----------------------------
def _json_safe(v):
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    try:
        if hasattr(v, "isoformat"):
            return v.isoformat()
    except Exception:
        pass
    try:
        return str(v)
    except Exception:
        return repr(v)


def _records_to_latest_events(records):
    """
    Converte lista di FluxRecord in una struttura evento-based JSON-safe:
      - time, measurement, tags, fields
    """
    events = {}
    for r in records:
        t = r.get_time()
        meas = r.get_measurement()
        field = r.get_field()
        val = r.get_value()

        values = dict(r.values) if hasattr(r, "values") else {}

        unit = values.get("unit_id") or values.get("unitId") or values.get("slave_id") or values.get("slave")
        dev = values.get("device_id") or values.get("deviceId") or values.get("inv_id") or values.get("id")
        sid = values.get("site_id") or values.get("siteId")

        key = (meas, str(t), str(sid), str(unit) if unit is not None else "", str(dev) if dev is not None else "")
        if key not in events:
            tags = {
                k: _json_safe(v)
                for k, v in values.items()
                if k not in ("_value", "_field", "_time", "_start", "_stop", "result", "table")
            }
            events[key] = {
                "time": t.isoformat() if hasattr(t, "isoformat") else str(t),
                "measurement": _json_safe(meas),
                "tags": tags,
                "fields": {},
            }

        events[key]["fields"][str(field)] = _json_safe(val)

    out = list(events.values())
    out.sort(key=lambda e: e.get("time", ""), reverse=True)
    return out


def _infer_ok_from_event(ev: Dict[str, Any]) -> Optional[bool]:
    """
    Stato deterministico basato su come mqtt_ingest scrive i punti:
      - measurement == "modbus_error"  => ok=False
      - measurement == "modbus_status" => ok = fields["modbus_ok"] (se presente)
    """
    if not ev:
        return None

    meas = (ev.get("measurement") or "").strip().lower()
    fields = ev.get("fields") or {}

    if meas == "modbus_error":
        return False

    if "modbus_ok" in fields:
        v = fields.get("modbus_ok")
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "yes", "ok")

    return None


def _influx_cfg() -> Dict[str, str]:
    url = os.getenv("INFLUX_URL", "http://influxdb:8086")
    token = os.getenv("INFLUX_TOKEN", "")
    org = os.getenv("INFLUX_ORG", "")
    bucket = os.getenv("INFLUX_BUCKET", "")
    return {"url": url, "token": token, "org": org, "bucket": bucket}


@router.get("/site/{site_id}/devices/status")
def get_devices_status(site_id: str):
    """
    Stato per-device basato su measurement modbus_status/modbus_error (ultimi eventi).
    """
    try:
        sid = _normalize_site_id(site_id)

        with _LOCK:
            reg = _read_registry()
            site = reg["sites"].get(sid) or {"devices": []}
            devices = site.get("devices") or []

        cfg = _influx_cfg()
        if not (cfg["token"] and cfg["org"] and cfg["bucket"]):
            raise HTTPException(
                status_code=500,
                detail="Influx env missing (INFLUX_TOKEN/INFLUX_ORG/INFLUX_BUCKET). Cannot compute devices status.",
            )

        flux = f"""
from(bucket: "{cfg['bucket']}")
|> range(start: -7d)
|> filter(fn: (r) => r.site_id == "{sid}")
|> filter(fn: (r) => r._measurement == "modbus_status" or r._measurement == "modbus_error")
|> sort(columns: ["_time"], desc: true)
|> limit(n: 300)
""".strip()

        events = []
        try:
            with InfluxDBClient(url=cfg["url"], token=cfg["token"], org=cfg["org"]) as c:
                tables = c.query_api().query(flux)
                recs = []
                for t in tables:
                    recs.extend(list(t.records))
                events = _records_to_latest_events(recs)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Influx query failed: {e}")

        # indicizza per unit_id
        by_unit: Dict[str, List[Dict[str, Any]]] = {}
        for ev in events:
            tags = ev.get("tags") or {}
            unit = tags.get("unit_id") or tags.get("unitId") or tags.get("slave_id") or tags.get("slave")
            if unit is None:
                continue
            u = str(unit)
            by_unit.setdefault(u, []).append(ev)

        out_devices = []
        for d in devices:
            unit_id = d.get("unit_id")
            evs = by_unit.get(str(unit_id)) if unit_id is not None else None
            ev_pick = (evs[0] if evs else None)

            out_devices.append(
                {
                    "id": d.get("id"),
                    "name": d.get("name"),
                    "host": d.get("host"),
                    "port": d.get("port"),
                    "unit_id": unit_id,
                    "enabled": d.get("enabled", True),
                    "model": d.get("model", "sunspec"),
                    "ok": _infer_ok_from_event(ev_pick) if ev_pick else None,
                    "last_event": ev_pick,
                }
            )

        return {
            "site_id": sid,
            "computed_from": "influx.modbus_status/modbus_error",
            "now": datetime.now(timezone.utc).isoformat(),
            "devices": out_devices,
        }

    except HTTPException:
        raise
    except Exception as e:
        print("[devices_api] get_devices_status FAILED:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"get_devices_status failed: {e}")


# =========================
# Device telemetry (latest / series) for dashboard inverter view
# =========================
def _get_cfg_devices_registry(site_id: str) -> List[Dict[str, Any]]:
    """
    Ritorna i devices configurati nel registry, filtrando enabled.
    """
    sid = _normalize_site_id(site_id)
    with _LOCK:
        reg = _read_registry()
        site = reg["sites"].get(sid) or {"devices": []}
        devs = site.get("devices") or []
    out = []
    for d in devs:
        if (d.get("enabled") is None) or bool(d.get("enabled")):
            out.append(d)
    return out


def _map_request_to_influx_id(site_id: str, requested_device_id: str) -> str:
    """
    UI -> valore del tag Influx.
    Nel tuo caso Influx tagga "device" con il *name* (KACO_1), non con l'id (dev1).
    Quindi:
      - se chiedi dev1 => torna KACO_1 (se presente in registry)
      - se chiedi KACO_1 => torna KACO_1
    """
    req = str(requested_device_id or "").strip()
    if not req:
        return req

    devs = _get_cfg_devices_registry(site_id)

    # match per id
    for d in devs:
        if str(d.get("id", "")).strip() == req:
            name = str(d.get("name", "") or "").strip()
            return name or req

    # match per name (se già passi KACO_1)
    for d in devs:
        if str(d.get("name", "")).strip() == req:
            return req

    return req


def _age_s(ts_iso: Optional[str]) -> Optional[int]:
    if not ts_iso:
        return None
    try:
        dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
        return max(0, int((datetime.now(timezone.utc) - dt).total_seconds()))
    except Exception:
        return None


def _influx_query_api() -> Any:
    """
    Usa il client condiviso dell'app (come pv_aggregate_api.py).
    """
    c: InfluxDBClient = get_influx_client()
    return c.query_api()


@router.get("/site/{site_id}/device/{device_id}/latest")
@router.get("/site/{site_id}/devices/{device_id}/latest")  # compat vecchio path
def device_latest(site_id: str, device_id: str, lookback_s: int = Query(86400, ge=60, le=7 * 86400)):
    """
    Latest telemetry point per singolo inverter.
    """
    sid = _normalize_site_id(site_id)
    influx_dev = _map_request_to_influx_id(sid, device_id)

    try:
        q = _influx_query_api()

        flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{lookback_s}s)
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")
  |> filter(fn: (r) => r["{TAG_SITE}"] == "{sid}")
  |> filter(fn: (r) => r["{TAG_DEVICE}"] == "{influx_dev}")
  |> last()
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
""".strip()

        tables = q.query(flux, org=INFLUX_ORG)

        row = None
        for t in tables or []:
            for r in t.records:
                row = r.values
                break
            if row:
                break

        if not row:
            return {
                "site_id": sid,
                "device_id": device_id,
                "device_influx": influx_dev,
                "ok": None,
                "last_ts": None,
                "age_s": None,
                "fields": {},
                "raw": None,
            }

        ts = row.get("_time")
        if hasattr(ts, "isoformat"):
            ts_iso = ts.isoformat().replace("+00:00", "Z")
        else:
            ts_iso = str(ts)

        fields: Dict[str, Any] = {}
        for k in ("p_ac_w", "grid_v", "freq_hz", "i_ac_a", "v_dc_v", "v_dc"):
            if k in row and row.get(k) is not None:
                fields[k] = row.get(k)

        return {
            "site_id": sid,
            "device_id": device_id,
            "device_influx": influx_dev,
            "ok": True,
            "last_ts": ts_iso,
            "age_s": _age_s(ts_iso),
            "fields": fields,
            "raw": row,
        }

    except Exception as e:
        print("[devices_api] device_latest FAILED:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"device_latest failed: {e}")


@router.get("/site/{site_id}/device/{device_id}/series")
@router.get("/site/{site_id}/devices/{device_id}/series")  # compat vecchio path
def device_series(
    site_id: str,
    device_id: str,
    minutes: int = Query(120, ge=5, le=7 * 24 * 60),
    every: str = Query("10s"),
):
    """
    Timeseries per singolo device (p_ac_w).
    """
    sid = _normalize_site_id(site_id)
    influx_dev = _map_request_to_influx_id(sid, device_id)

    try:
        q = _influx_query_api()

        flux = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{minutes}m)
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")
  |> filter(fn: (r) => r["{TAG_SITE}"] == "{sid}")
  |> filter(fn: (r) => r["{TAG_DEVICE}"] == "{influx_dev}")
  |> filter(fn: (r) => r._field == "p_ac_w")
  |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
  |> keep(columns: ["_time","_value"])
""".strip()

        tables = q.query(flux, org=INFLUX_ORG)

        t_out: List[str] = []
        pac: List[Optional[float]] = []
        for tb in tables or []:
            for rec in tb.records:
                ts = rec.get_time()
                if not ts:
                    continue
                t_out.append(ts.isoformat().replace("+00:00", "Z"))
                v = rec.get_value()
                pac.append(float(v) if isinstance(v, (int, float)) else None)

        return {
            "site_id": sid,
            "device_id": device_id,
            "device_influx": influx_dev,
            "t": t_out,
            "p_ac_w": pac,
            "minutes": minutes,
            "every": every,
        }

    except Exception as e:
        print("[devices_api] device_series FAILED:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"device_series failed: {e}")
