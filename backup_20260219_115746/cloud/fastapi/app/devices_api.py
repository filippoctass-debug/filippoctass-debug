from __future__ import annotations

import json
import os
import tempfile
import threading
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from influxdb_client import InfluxDBClient


router = APIRouter(prefix="/api", tags=["devices"])

def _bm_dump(m):
    # compat pydantic v1/v2
    if hasattr(m, "model_dump"):
        return m.model_dump()
    if hasattr(m, "dict"):
        return m.dict()
    return dict(m)


_LOCK = threading.Lock()

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

    # IMPORTANT:
    # temp file MUST be created in the SAME directory as target file,
    # otherwise os.replace() may fail across different filesystems (EXDEV).
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
    # id interno (stringa) che useremo anche per setpoint target
    id: str = Field(..., min_length=1, max_length=64)
    name: str = Field("inverter", max_length=128)

    host: str = Field(..., min_length=1, max_length=255)  # ip/dns
    port: int = Field(502, ge=1, le=65535)
    unit_id: int = Field(1, ge=0, le=247)

    enabled: bool = True

    # per ora fissiamo Sunspec (poi estendiamo)
    model: str = Field("sunspec", max_length=32)


class DevicesPayload(BaseModel):
    site_id: str = Field(..., min_length=1, max_length=64)
    devices: List[ModbusDevice] = Field(default_factory=list)

def _normalize_site_id(site_id: str) -> str:
    return str(site_id).strip()

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

        return {"site_id": sid, "devices": devices_out, "saved": True}

    except HTTPException:
        raise
    except Exception as e:
        # stampa traceback in log container
        print("[devices_api] PUT FAILED:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"put_devices failed: {e}")

def _influx_cfg() -> Dict[str, str]:
    # allineato a mqtt_ingest (env)
    url = os.getenv("INFLUX_URL", "http://influxdb:8086")
    token = os.getenv("INFLUX_TOKEN", "")
    org = os.getenv("INFLUX_ORG", "")
    bucket = os.getenv("INFLUX_BUCKET", "")
    return {"url": url, "token": token, "org": org, "bucket": bucket}


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
            tags = {k: _json_safe(v) for k, v in values.items()
                    if k not in ("_value", "_field", "_time", "_start", "_stop", "result", "table")}
            events[key] = {
                "time": t.isoformat() if hasattr(t, "isoformat") else str(t),
                "measurement": _json_safe(meas),
                "tags": tags,
                "fields": {},
            }

        # fields sempre JSON-safe
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

    # se l'ultimo è un errore, è KO
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

    # se è status ma manca modbus_ok, non inventiamo
    return None

@router.get("/site/{site_id}/devices/status")

def get_devices_status(site_id: str):
    try:
        sid = _normalize_site_id(site_id)

        # devices configurati
        with _LOCK:
            reg = _read_registry()
            site = reg["sites"].get(sid) or {"devices": []}
            devices = site.get("devices") or []

        cfg = _influx_cfg()
        if not (cfg["token"] and cfg["org"] and cfg["bucket"]):
            raise HTTPException(
                status_code=500,
                detail="Influx env missing (INFLUX_TOKEN/INFLUX_ORG/INFLUX_BUCKET). Cannot compute devices status from modbus_status.",
            )

        flux = f"""
from(bucket: "{cfg['bucket']}")
|> range(start: -7d)
|> filter(fn: (r) => r.site_id == "{sid}")
|> filter(fn: (r) => r._measurement == "modbus_status" or r._measurement == "modbus_error")
|> sort(columns: ["_time"], desc: true)
|> limit(n: 300)
"""

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

        # indicizza per unit_id tag
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

            out_devices.append({
                "id": d.get("id"),
                "name": d.get("name"),
                "host": d.get("host"),
                "port": d.get("port"),
                "unit_id": unit_id,
                "enabled": d.get("enabled", True),
                "model": d.get("model", "sunspec"),
                "ok": _infer_ok_from_event(ev_pick) if ev_pick else None,
                "last_event": ev_pick,
            })

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

def _json_safe(v):
    # riduce qualsiasi tipo a qualcosa serializzabile in JSON
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    try:
        # datetime, date, ecc.
        if hasattr(v, "isoformat"):
            return v.isoformat()
    except Exception:
        pass
    try:
        return str(v)
    except Exception:
        return repr(v)





### DEVICE LATEST/SERIES API ###
# NOTE:
# - Questi endpoint servono la dashboard per la vista inverter.
# - Query Influx: measurement pv_telemetry_v2 con tag site_id + device_id.
# - Se i tuoi nomi bucket/org/url/token sono diversi, usa env:
#   INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET

from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
import os

try:
    from influxdb_client import InfluxDBClient
except Exception:
    InfluxDBClient = None  # type: ignore

_INFLUX = {"client": None, "q": None, "org": None, "bucket": None}

def _influx_q():
    if _INFLUX["q"] is not None:
        return _INFLUX["q"]
    if InfluxDBClient is None:
        raise RuntimeError("influxdb_client non disponibile. Aggiungi 'influxdb-client' in requirements.")
    url = os.getenv("INFLUX_URL", "http://influxdb:8086")
    token = os.getenv("INFLUX_TOKEN", "")
    org = os.getenv("INFLUX_ORG", "")
    bucket = os.getenv("INFLUX_BUCKET", os.getenv("INFLUXDB_BUCKET", "ctass"))
    if not token or not org or not bucket:
        raise RuntimeError("Config Influx mancante: set INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET (e INFLUX_URL se serve).")
    c = InfluxDBClient(url=url, token=token, org=org, timeout=10_000)
    _INFLUX["client"] = c
    _INFLUX["q"] = c.query_api()
    _INFLUX["org"] = org
    _INFLUX["bucket"] = bucket
    return _INFLUX["q"]

def _age_s(ts_iso: Optional[str]) -> Optional[int]:
    if not ts_iso:
        return None
    try:
        dt = datetime.fromisoformat(ts_iso.replace("Z","+00:00"))
        return max(0, int((datetime.now(timezone.utc) - dt).total_seconds()))
    except Exception:
        return None

@router.get("/site/{site_id}/devices/{device_id}/latest")
def device_latest(site_id: str, device_id: str):
    q = _influx_q()
    org = _INFLUX["org"]
    bucket = _INFLUX["bucket"]

    flux = f'''
from(bucket: "{bucket}")
  |> range(start: -24h)
  |> filter(fn: (r) => r["_measurement"] == "pv_telemetry_v2")
  |> filter(fn: (r) => r["site_id"] == "{site_id}")
  |> filter(fn: (r) => r["device_id"] == "{device_id}")
  |> last()
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time","p_ac_w","grid_v","freq_hz","i_ac_a","v_dc_v","v_dc","manufacturer","model","serial","source","sunspec_base"])
'''
    tables = q.query(query=flux, org=org)
    row = None
    for t in tables:
        for r in t.records:
            row = r.values
            break

    if not row:
        return {
            "site_id": site_id,
            "device_id": device_id,
            "ok": None,
            "last_ts": None,
            "age_s": None,
            "fields": {},
            "raw": None,
        }

    ts = row.get("_time")
    if hasattr(ts, "isoformat"):
        ts_iso = ts.isoformat().replace("+00:00","Z")
    else:
        ts_iso = str(ts)

    # Estrai fields noti + fallback generico numerico
    fields: Dict[str, Any] = {}
    for k in ("p_ac_w","grid_v","freq_hz","i_ac_a","v_dc_v","v_dc"):
        if k in row and row.get(k) is not None:
            fields[k] = row.get(k)

    meta: Dict[str, Any] = {}
    for k in ("manufacturer","model","serial","source","sunspec_base"):
        if k in row and row.get(k) is not None:
            meta[k] = row.get(k)

    return {
        "site_id": site_id,
        "device_id": device_id,
        "ok": True,
        "last_ts": ts_iso,
        "age_s": _age_s(ts_iso),
        "fields": fields,
        "meta": meta,
        "raw": row,
    }

@router.get("/site/{site_id}/devices/{device_id}/series")
def device_series(site_id: str, device_id: str, minutes: int = 120, every: str = "10s"):
    q = _influx_q()
    org = _INFLUX["org"]
    bucket = _INFLUX["bucket"]
    minutes = max(1, min(int(minutes), 24*60))

    flux = f'''
from(bucket: "{bucket}")
  |> range(start: -{minutes}m)
  |> filter(fn: (r) => r["_measurement"] == "pv_telemetry_v2")
  |> filter(fn: (r) => r["site_id"] == "{site_id}")
  |> filter(fn: (r) => r["device_id"] == "{device_id}")
  |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time","p_ac_w","grid_v","freq_hz","i_ac_a","v_dc_v","v_dc"])
'''
    tables = q.query(query=flux, org=org)

    t_out: List[str] = []
    out = {
        "site_id": site_id,
        "device_id": device_id,
        "t": t_out,
        "p_ac_w": [],
        "grid_v": [],
        "freq_hz": [],
        "i_ac_a": [],
        "v_dc_v": [],
        "v_dc": [],
    }

    for tb in tables:
        for rec in tb.records:
            v = rec.values
            ts = v.get("_time")
            if hasattr(ts, "isoformat"):
                ts_iso = ts.isoformat().replace("+00:00","Z")
            else:
                ts_iso = str(ts)
            t_out.append(ts_iso)
            for k in ("p_ac_w","grid_v","freq_hz","i_ac_a","v_dc_v","v_dc"):
                out[k].append(v.get(k))

    return out

# ------------------------
# Per-device endpoints (latest / series)
# ------------------------
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG = os.getenv("INFLUX_ORG", "")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", os.getenv("INFLUXDB_BUCKET", ""))

def _influx_query(flux: str):
    if not INFLUX_TOKEN or not INFLUX_ORG or not INFLUX_BUCKET:
        raise RuntimeError("Influx env missing. Set INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET (or INFLUXDB_BUCKET).")
    cli = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=10_000)
    try:
        q = cli.query_api()
        return q.query(flux, org=INFLUX_ORG)
    finally:
        try: cli.close()
        except Exception: pass

@router.get("/site/{site_id}/devices/{device_id}/latest")
def device_latest(site_id: str, device_id: str):
    """
    Latest telemetry point for a single inverter/device.
    Reads measurement pv_telemetry_v2 written by edge-driver with tag device_id.
    """
    minutes = 60 * 24  # search last 24h for the latest point
    flux = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{minutes}m)
  |> filter(fn: (r) => r._measurement == "pv_telemetry_v2")
  |> filter(fn: (r) => r.site_id == "{site_id}")
  |> filter(fn: (r) => r.device_id == "{device_id}")
  |> last()
'''
    tables = _influx_query(flux)

    fields = {}
    ts = None
    tags = {}
    for t in tables:
        for r in t.records:
            ts = r.get_time().isoformat() if r.get_time() else ts
            # fields are returned as _field/_value
            f = r.get_field()
            v = r.get_value()
            if f:
                fields[f] = v
            # best-effort tags
            for k in ("device", "device_id", "modbus_host", "modbus_port", "unit_id", "source"):
                try:
                    vv = r.values.get(k)
                    if vv is not None:
                        tags[k] = vv
                except Exception:
                    pass

    return {"site_id": site_id, "device_id": device_id, "ts": ts, "fields": fields, "tags": tags}

@router.get("/site/{site_id}/devices/{device_id}/series")
def device_series(site_id: str, device_id: str, minutes: int = 120, every: str = "10s"):
    """
    Timeseries for a single device (p_ac_w only for now, extend later).
    """
    minutes = max(5, min(int(minutes), 7*24*60))
    every = (every or "10s").strip()

    flux = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{minutes}m)
  |> filter(fn: (r) => r._measurement == "pv_telemetry_v2")
  |> filter(fn: (r) => r.site_id == "{site_id}")
  |> filter(fn: (r) => r.device_id == "{device_id}")
  |> filter(fn: (r) => r._field == "p_ac_w")
  |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
  |> yield(name: "mean")
'''
    tables = _influx_query(flux)

    t_out = []
    pac = []
    for t in tables:
        for r in t.records:
            tt = r.get_time()
            vv = r.get_value()
            if tt is None:
                continue
            t_out.append(tt.isoformat())
            pac.append(vv)

    return {"site_id": site_id, "device_id": device_id, "t": t_out, "p_ac_w": pac}
