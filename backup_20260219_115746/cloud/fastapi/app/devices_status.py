from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter

# devices list (registry)
try:
    from app.devices_store import get_site_devices  # preferred (if exists)
except Exception:
    get_site_devices = None  # fallback later

router = APIRouter(prefix="/api", tags=["devices-status"])


def _normalize_site_id(site_id: str) -> str:
    return str(site_id).strip()


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# -----------------------
# 1) Try in-memory cache
# -----------------------
def _try_get_cached_modbus_status(site_id: str) -> Optional[Dict[str, Any]]:
    """
    Tries to read the latest modbus_status already cached in memory by mqtt_ingest/status module.
    We don't assume exact variable names: we probe a few common ones.
    Returns the raw status payload dict if found.
    """
    sid = _normalize_site_id(site_id)

    try:
        import app.status as st  # many projects keep last messages here
    except Exception:
        st = None

    candidates: List[Any] = []
    if st is not None:
        # common patterns
        for name in [
            "LAST",
            "LAST_BY_SITE",
            "STATUS",
            "SITE_STATUS",
            "LATEST",
            "CACHE",
        ]:
            if hasattr(st, name):
                candidates.append(getattr(st, name))

    # Scan dict-like caches for modbus_status
    for c in candidates:
        if isinstance(c, dict):
            # patterns:
            # cache[sid]["modbus_status"]
            # cache["modbus_status"][sid]
            try:
                v = c.get(sid)
                if isinstance(v, dict) and "modbus_status" in v:
                    raw = v.get("modbus_status")
                    if isinstance(raw, dict):
                        return raw
            except Exception:
                pass

            try:
                ms = c.get("modbus_status")
                if isinstance(ms, dict):
                    raw = ms.get(sid)
                    if isinstance(raw, dict):
                        return raw
            except Exception:
                pass

    return None


# -----------------------
# 2) Fallback: query Influx
# -----------------------
def _try_get_influx_last_modbus_status(site_id: str) -> Optional[Dict[str, Any]]:
    """
    Reads the latest modbus_status from Influx.
    Works even if schema varies:
      - if there's a field 'payload' containing JSON -> parse it
      - else return a dict built from fields found
    """
    sid = _normalize_site_id(site_id)

    try:
        from influxdb_client import InfluxDBClient
    except Exception:
        return None

    url = os.getenv("INFLUX_URL", "http://influxdb:8086")
    token = os.getenv("INFLUX_TOKEN", "")
    org = os.getenv("INFLUX_ORG", "")
    bucket = os.getenv("INFLUX_BUCKET", "")

    if not (token and org and bucket):
        # if your project wraps these elsewhere, you can wire it later
        return None

    # measurement name guess: adjust if your project uses a different one
    meas_candidates = ["modbus_status", "pv_modbus_status", "pv_modbus_status_v1", "pv_modbus_status_v2"]

    with InfluxDBClient(url=url, token=token, org=org) as client:
        qapi = client.query_api()

        for meas in meas_candidates:
            flux = f'''
from(bucket: "{bucket}")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "{meas}")
  |> filter(fn: (r) => r.site_id == "{sid}")
  |> last()
'''
            try:
                tables = qapi.query(flux)
            except Exception:
                continue

            # pick first record
            for t in tables:
                for r in t.records:
                    # if payload JSON exists
                    try:
                        if r.get_field() == "payload":
                            v = r.get_value()
                            if isinstance(v, str) and v.strip():
                                return json.loads(v)
                    except Exception:
                        pass

                    # otherwise: build minimal dict
                    try:
                        return {
                            "site_id": sid,
                            "time": r.get_time().isoformat() if r.get_time() else None,
                            "field": r.get_field(),
                            "value": r.get_value(),
                            "tags": dict(r.values),
                        }
                    except Exception:
                        return {"site_id": sid, "raw": str(r.values)}

    return None


def _load_devices(site_id: str) -> List[Dict[str, Any]]:
    sid = _normalize_site_id(site_id)

    if get_site_devices is not None:
        try:
            return list(get_site_devices(sid) or [])
        except Exception:
            pass

    # fallback: call devices_api registry reader
    try:
        from app.devices_api import _read_registry, _storage_path  # type: ignore
        reg = _read_registry()
        site = (reg.get("sites") or {}).get(sid) or {}
        return list(site.get("devices") or [])
    except Exception:
        return []


def _map_status_to_devices(devices: List[Dict[str, Any]], status: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normalize modbus_status into per-device status objects.
    We support multiple payload shapes:
      A) {"devices":[{"id":"inv_1","online":true,"error":""}, ...], "ts":...}
      B) {"inv_1": {...}, "inv_2": {...}}
      C) unknown -> everything offline with raw attached
    """
    by_id: Dict[str, Dict[str, Any]] = {}

    if isinstance(status, dict):
        if isinstance(status.get("devices"), list):
            for d in status["devices"]:
                if isinstance(d, dict) and d.get("id"):
                    by_id[str(d["id"])] = d
        else:
            # dict keyed by device id
            for k, v in status.items():
                if isinstance(v, dict) and k not in ("site_id", "ts", "time", "timestamp"):
                    by_id[str(k)] = v

    out: List[Dict[str, Any]] = []
    for dev in devices:
        did = str(dev.get("id", "")).strip()
        s = by_id.get(did, {})

        # best-effort online flag
        online = False
        for key in ("online", "connected", "ok", "alive"):
            if key in s:
                online = bool(s.get(key))
                break

        item = {
            "id": did,
            "name": dev.get("name"),
            "online": online,
            "error": s.get("error") or s.get("err") or s.get("message") or None,
            "last_seen": s.get("ts") or s.get("time") or s.get("timestamp") or None,
        }
        out.append(item)

    # if we got a status but couldn't map it, attach raw once for debugging
    if status and not by_id:
        out.append({"id": "__raw__", "online": False, "raw": status})

    return out


@router.get("/site/{site_id}/devices/status")
def get_devices_status(site_id: str):
    sid = _normalize_site_id(site_id)
    devices = _load_devices(sid)

    status = _try_get_cached_modbus_status(sid)
    source = "cache"

    if status is None:
        status = _try_get_influx_last_modbus_status(sid)
        source = "influx" if status is not None else "none"

    return {
        "site_id": sid,
        "source": source,
        "ts": _iso_now(),
        "devices": _map_status_to_devices(devices, status if isinstance(status, dict) else None),
    }
