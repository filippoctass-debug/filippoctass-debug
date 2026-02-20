from __future__ import annotations

import json
import os
import tempfile
import traceback
import time
import threading
from pathlib import Path
from typing import Any, Dict, Optional, List
from urllib.parse import urlencode
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

router = APIRouter(tags=["weather"])

# -----------------------------
# Storage (JSON file per-site)
# -----------------------------
_LOCK = threading.Lock()

def _storage_path() -> Path:
    here = Path(__file__).resolve().parent
    return here / "storage" / "weather_cfg.json"

def _read_cfg_all() -> Dict[str, Any]:
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
        return {"version": 1, "sites": {}}

def _atomic_write(p: Path, data: Dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    with tempfile.NamedTemporaryFile(
        "w",
        delete=False,
        encoding="utf-8",
        newline="\n",
        dir=str(p.parent),
        prefix=".tmp_weather_cfg_",
        suffix=".json",
    ) as f:
        f.write(payload)
        tmp = f.name
    os.replace(tmp, str(p))

def _norm_site(site_id: str) -> str:
    return str(site_id).strip()

def _parse_float(x: str, name: str) -> float:
    try:
        v = float(str(x).strip())
        if not (v == v):  # NaN
            raise ValueError()
        return v
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid {name}")

def _get_site_cfg(site_id: str) -> Dict[str, Any]:
    sid = _norm_site(site_id)
    with _LOCK:
        reg = _read_cfg_all()
        site = (reg.get("sites") or {}).get(sid) or {}
        cfg = site.get("cfg") or {}
    if not isinstance(cfg, dict):
        cfg = {}
    return cfg

def _set_site_cfg(site_id: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    sid = _norm_site(site_id)
    with _LOCK:
        reg = _read_cfg_all()
        reg.setdefault("version", 1)
        reg.setdefault("sites", {})
        reg["sites"][sid] = {"cfg": cfg}
        _atomic_write(_storage_path(), reg)
    return cfg

# -----------------------------
# Open-Meteo call + cache/limit
# -----------------------------
# Cache in memoria per evitare rate-limit e rendere UI stabile.
# key: debug_url (include lat/lon/tz/fields)
_WEATHER_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_LOCK = threading.Lock()

CACHE_TTL_S = int(os.getenv("WEATHER_CACHE_TTL_S", "60"))          # cache "fresh"
CACHE_STALE_MAX_S = int(os.getenv("WEATHER_CACHE_STALE_MAX_S", "900"))  # 15 min di fallback
HTTP_TIMEOUT_S = int(os.getenv("WEATHER_HTTP_TIMEOUT_S", "15"))

def _now_s() -> float:
    return time.time()

def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    with _CACHE_LOCK:
        it = _WEATHER_CACHE.get(key)
        if not it:
            return None
        return dict(it)

def _cache_set(key: str, payload: Dict[str, Any]) -> None:
    with _CACHE_LOCK:
        _WEATHER_CACHE[key] = {
            "ts": _now_s(),
            "payload": payload,
        }

def _http_json(url: str, timeout_s: int = HTTP_TIMEOUT_S) -> Dict[str, Any]:
    """
    Ritorna dict JSON da Open‑Meteo.
    Gestisce 429 con errore specifico (non 502 generico).
    """
    try:
        req = Request(url, headers={"User-Agent": "control-room/1.0"})
        with urlopen(req, timeout=timeout_s) as r:
            raw = r.read().decode("utf-8", errors="replace")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("non-dict json")
        return data

    except HTTPError as e:
        # e.code esiste
        if e.code == 429:
            raise HTTPException(status_code=429, detail="Open-Meteo rate limit (429)")
        # altri HTTP
        raise HTTPException(status_code=502, detail=f"Open-Meteo HTTP error {e.code}")

    except URLError as e:
        raise HTTPException(status_code=502, detail=f"Open-Meteo network error: {e}")

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Open-Meteo fetch failed: {e}")

def _build_open_meteo_url(lat: float, lon: float, tz: str) -> str:
    params = {
        "latitude": lat,
        "longitude": lon,
        "timezone": tz,
        "current": "temperature_2m,cloud_cover,uv_index",
        "hourly": "cloud_cover,uv_index,shortwave_radiation",
    }
    return "https://api.open-meteo.com/v1/forecast?" + urlencode(params)

def _compute_payload(site_id: str, cfg: Dict[str, Any], j: Dict[str, Any], url: str) -> Dict[str, Any]:
    hourly = j.get("hourly") or {}
    times: List[str] = hourly.get("time") or []
    sw: List[Optional[float]] = hourly.get("shortwave_radiation") or []
    cloud: List[Optional[float]] = hourly.get("cloud_cover") or []
    uv: List[Optional[float]] = hourly.get("uv_index") or []

    kwp = float(cfg.get("pv_kwp") or 10.0)
    pr = float(cfg.get("pr") or 0.85)

    # Production estimate (kW): radiation(W/m2)/1000 * kwp * pr
    p_kw: List[Optional[float]] = []
    for v in sw:
        if isinstance(v, (int, float)):
            p_kw.append(max(0.0, (float(v) / 1000.0) * kwp * pr))
        else:
            p_kw.append(None)

    return {
        "site_id": site_id,
        "cfg": cfg,
        "current": j.get("current") or {},
        "hourly": {
            "time": times,
            "cloud_cover": cloud,
            "uv_index": uv,
            "shortwave_radiation": sw,
        },
        "production_est_kw": {
            "time": times,
            "p_kw": p_kw,
            "kwp": kwp,
            "pr": pr,
        },
        "source": "open-meteo",
        "debug_url": url,
    }

# -----------------------------
# API
# -----------------------------
@router.get("/site/{site_id}/weather_cfg")
def weather_cfg_get(site_id: str):
    cfg = _get_site_cfg(site_id)
    return {"site_id": _norm_site(site_id), "cfg": cfg}

@router.put("/site/{site_id}/weather_cfg")
def weather_cfg_put(
    site_id: str,
    lat: str = Query(...),
    lon: str = Query(...),
    pv_kwp: str = Query("10"),
    timezone: str = Query("UTC"),
    pr: str = Query("0.85"),
):
    sid = _norm_site(site_id)
    lat_f = _parse_float(lat, "lat")
    lon_f = _parse_float(lon, "lon")
    kwp_f = _parse_float(pv_kwp, "pv_kwp")
    pr_f = _parse_float(pr, "pr")

    cfg = {
        "lat": lat_f,
        "lon": lon_f,
        "pv_kwp": kwp_f,
        "timezone": (timezone or "UTC").strip() or "UTC",
        "pr": pr_f,
    }
    _set_site_cfg(sid, cfg)
    return {"site_id": sid, "cfg": cfg, "saved": True}

@router.get("/site/{site_id}/weather")
def weather(site_id: str):
    sid = _norm_site(site_id)
    cfg = _get_site_cfg(sid)

    lat = cfg.get("lat")
    lon = cfg.get("lon")
    tz = (cfg.get("timezone") or "UTC").strip() or "UTC"

    if lat is None or lon is None:
        # IMPORTANT: UI si aspetta 400 quando non configurato
        raise HTTPException(status_code=400, detail="weather cfg missing (lat/lon). Set /weather_cfg first.")

    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except Exception:
        raise HTTPException(status_code=400, detail="weather cfg invalid (lat/lon not numeric).")

    url = _build_open_meteo_url(lat_f, lon_f, tz)

    # 1) cache fresh
    cached = _cache_get(url)
    if cached:
        age = _now_s() - float(cached.get("ts", 0))
        if age <= CACHE_TTL_S:
            payload = dict(cached.get("payload") or {})
            payload["cached"] = True
            payload["cache_age_s"] = int(age)
            return payload

    # 2) fetch live
    try:
        j = _http_json(url)
        payload = _compute_payload(sid, cfg, j, url)
        _cache_set(url, payload)
        payload["cached"] = False
        payload["cache_age_s"] = 0
        return payload

    except HTTPException as e:
        # 3) fallback: se rate-limited o errore rete, prova cache "stale"
        cached = _cache_get(url)
        if cached:
            age = _now_s() - float(cached.get("ts", 0))
            if age <= CACHE_STALE_MAX_S:
                payload = dict(cached.get("payload") or {})
                payload["cached"] = True
                payload["cache_age_s"] = int(age)
                payload["stale"] = True
                payload["stale_reason"] = str(e.detail)
                return payload

        # niente cache -> ritorna errore "pulito"
        if e.status_code == 429:
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "Open-Meteo rate limited (429) and no cache available",
                    "retry_after_s": 60,
                    "debug_url": url,
                    "site_id": sid,
                },
                headers={"Retry-After": "60"},
            )

        # lascia passare gli altri (502, ecc.)
        raise

    except Exception as e:
        print("[weather_api] unexpected error:", repr(e))
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"weather handler failed: {e}")
