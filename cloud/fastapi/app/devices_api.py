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
    Converte lista di FluxRecord in una struttura più 'evento-based'.
    Ritorna lista eventi con:
      - time, measurement, tags, fields
    """
    events = {}
    for r in records:
        t = r.get_time()
        meas = r.get_measurement()
        field = r.get_field()
        val = r.get_value()
        # tags/values disponibili in r.values
        values = dict(r.values) if hasattr(r, "values") else {}

        # chiave evento: (measurement, time, tagset "stabile")
        # proviamo a usare unit_id/device_id se presenti, sennò solo site_id
        unit = values.get("unit_id") or values.get("unitId") or values.get("slave_id") or values.get("slave")
        dev = values.get("device_id") or values.get("deviceId") or values.get("inv_id") or values.get("id")
        sid = values.get("site_id") or values.get("siteId")

        key = (meas, str(t), str(sid), str(unit) if unit is not None else "", str(dev) if dev is not None else "")
        if key not in events:
            # separa tags "puliti" dai campi flux
            tags = {k: v for k, v in values.items()
                    if k not in ("_value", "_field", "_time", "_start", "_stop", "result", "table")}
            events[key] = {
                "time": t.isoformat() if hasattr(t, "isoformat") else str(t),
                "measurement": meas,
                "tags": tags,
                "fields": {},
            }
        events[key]["fields"][field] = val

    # ordina per time desc
    out = list(events.values())
    out.sort(key=lambda e: e.get("time", ""), reverse=True)
    return out


def _infer_ok_from_event(ev: Dict[str, Any]) -> Optional[bool]:
    """
    Tenta di inferire 'ok' da fields/tags tipici.
    Se non riesce, None.
    """
    fields = (ev or {}).get("fields") or {}
    tags = (ev or {}).get("tags") or {}

    # segnali positivi
    for k in ("ok", "success", "connected", "online", "alive"):
        if k in fields:
            v = fields[k]
            if isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return bool(v)
            if isinstance(v, str):
                return v.strip().lower() in ("1", "true", "yes", "ok", "success", "connected", "online")
        if k in tags:
            v = tags[k]
            if isinstance(v, str):
                return v.strip().lower() in ("1", "true", "yes", "ok", "success", "connected", "online")

    # segnali negativi
    for k in ("error", "fault", "failed"):
        if k in fields:
            v = fields[k]
            if isinstance(v, bool):
                return not v
            if isinstance(v, (int, float)):
                return v == 0
            if isinstance(v, str):
                return v.strip().lower() in ("0", "false", "no", "none", "")
        if k in tags:
            v = tags[k]
            if isinstance(v, str):
                return v.strip().lower() in ("0", "false", "no", "none", "")

    # error code numerico
    for k in ("err", "errno", "error_code", "code"):
        if k in fields and isinstance(fields[k], (int, float)):
            return fields[k] == 0

    return None


@router.get("/site/{site_id}/devices/status")
def get_devices_status(site_id: str):
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

    # Query: ultimi eventi modbus_status e modbus_error del sito (range ampio ma limit)
    flux = f"""
from(bucket: "{cfg['bucket']}")
|> range(start: -7d)
|> filter(fn: (r) => r.site_id == "{sid}")
|> filter(fn: (r) => r._measurement == "modbus_status" or r._measurement == "modbus_error")
|> sort(columns: ["_time"], desc: true)
|> limit(n: 300)
"""

    events = []
    with InfluxDBClient(url=cfg["url"], token=cfg["token"], org=cfg["org"]) as c:
        tables = c.query_api().query(flux)
        recs = []
        for t in tables:
            recs.extend(list(t.records))
        events = _records_to_latest_events(recs)

    # indicizza per unit_id (se presente negli eventi)
    by_unit: Dict[str, List[Dict[str, Any]]] = {}
    for ev in events:
        tags = ev.get("tags") or {}
        unit = tags.get("unit_id") or tags.get("unitId") or tags.get("slave_id") or tags.get("slave")
        if unit is None:
            continue
        u = str(unit)
        by_unit.setdefault(u, []).append(ev)

    # compone output per device
    out_devices = []
    for d in devices:
        unit_id = d.get("unit_id")
        evs = None
        if unit_id is not None:
            evs = by_unit.get(str(unit_id))
        # fallback: nessun match per unit -> usa i primi eventi del sito (se esistono)
        ev_pick = (evs[0] if evs else (events[0] if events else None))

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

