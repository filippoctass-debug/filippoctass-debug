import json
import os
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.influx import get_influx_client

_LOCK = threading.Lock()
REG_PATH = os.getenv("SITES_REGISTRY_PATH", "/app/app/storage/sites_registry.json")

def _ensure_dir():
    d = os.path.dirname(REG_PATH)
    if d and not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)

def _load_registry() -> Dict[str, Any]:
    _ensure_dir()
    if not os.path.isfile(REG_PATH):
        return {"sites": {}}
    try:
        with open(REG_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {"sites": {}}
    except Exception:
        return {"sites": {}}

def _save_registry(data: Dict[str, Any]) -> None:
    _ensure_dir()
    tmp = REG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, REG_PATH)

def register_site(site_id: str) -> None:
    site_id = str(site_id).strip()
    if not site_id:
        return
    now = datetime.now(timezone.utc).isoformat()
    with _LOCK:
        data = _load_registry()
        sites = data.setdefault("sites", {})
        if site_id not in sites:
            sites[site_id] = {"first_seen": now}
        _save_registry(data)

def delete_site(site_id: str) -> bool:
    site_id = str(site_id).strip()
    if not site_id:
        return False
    with _LOCK:
        data = _load_registry()
        sites = data.setdefault("sites", {})
        if site_id not in sites:
            return False
        sites.pop(site_id, None)
        _save_registry(data)
        return True

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _to_iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def _query_last_edge_time(lookback_days: int) -> Dict[str, datetime]:
    """
    Ultimo timestamp edge_status per site_id SENZA collisioni di schema:
    group per site_id + _field, last() per field, poi max per site_id lato python.
    """
    org = os.getenv("INFLUX_ORG", "")
    bucket = os.getenv("INFLUX_BUCKET", "")
    c = get_influx_client()

    flux = f'''
from(bucket:"{bucket}")
  |> range(start: -{int(lookback_days)}d)
  |> filter(fn: (r) => r._measurement == "edge_status")
  |> group(columns:["site_id","_field"])
  |> last()
'''
    tables = c.query_api().query(flux, org=org)

    out: Dict[str, datetime] = {}
    for t in tables:
        for r in t.records:
            sid = r.values.get("site_id")
            if not sid:
                continue
            sid = str(sid)
            rt = r.get_time()
            if not rt:
                continue
            prev = out.get(sid)
            if prev is None or rt > prev:
                out[sid] = rt
    return out

def _query_last_mqtt_connected(lookback_days: int) -> Dict[str, bool]:
    org = os.getenv("INFLUX_ORG", "")
    bucket = os.getenv("INFLUX_BUCKET", "")
    c = get_influx_client()

    flux = f'''
from(bucket:"{bucket}")
  |> range(start: -{int(lookback_days)}d)
  |> filter(fn: (r) => r._measurement == "edge_status")
  |> filter(fn: (r) => r._field == "mqtt_connected")
  |> group(columns:["site_id"])
  |> last()
'''
    tables = c.query_api().query(flux, org=org)

    out: Dict[str, bool] = {}
    for t in tables:
        for r in t.records:
            sid = r.values.get("site_id")
            if not sid:
                continue
            v = r.get_value()
            if isinstance(v, str):
                v_norm = v.strip().lower() in ("1", "true", "yes", "y", "on")
            else:
                v_norm = bool(v) if v is not None else False
            out[str(sid)] = v_norm
    return out

def list_sites(lookback_days: int = 7, online_within_seconds: int = 120) -> List[Dict[str, Any]]:
    with _LOCK:
        data = _load_registry()
        reg_sites = data.get("sites", {}) or {}
        known = sorted(reg_sites.keys())

    try:
        last_time = _query_last_edge_time(lookback_days=lookback_days)
    except Exception as e:
        print(f"[sites] WARN last_time query failed: {e}")
        last_time = {}

    try:
        mqtt_map = _query_last_mqtt_connected(lookback_days=lookback_days)
    except Exception as e:
        print(f"[sites] WARN mqtt_connected query failed: {e}")
        mqtt_map = {}

    now = _now_utc()
    out: List[Dict[str, Any]] = []

    for sid in known:
        t = last_time.get(sid)
        mqtt = mqtt_map.get(sid)  # None se mai scritto

        recent = False
        if t is not None:
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            age = (now - t).total_seconds()
            recent = age <= float(online_within_seconds)

        online = bool(recent and mqtt is True)

        out.append({
            "site_id": sid,
            "online": online,
            "last_edge_status": _to_iso(t),
            "mqtt_connected": mqtt,
        })

    return out
