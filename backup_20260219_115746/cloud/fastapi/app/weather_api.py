import os
import json
from typing import Any, Dict, Optional, List
from fastapi import APIRouter, Query, HTTPException
import httpx

router = APIRouter(tags=["weather"])

# Persisted config per-PV (salvato su volume app)
_CFG_PATH = os.getenv("WEATHER_CFG_PATH", "/app/app/data/weather_cfg.json")

def _ensure_cfg_dir() -> None:
    d = os.path.dirname(_CFG_PATH)
    if d:
        os.makedirs(d, exist_ok=True)

def _load_cfg_all() -> Dict[str, Any]:
    _ensure_cfg_dir()
    if not os.path.exists(_CFG_PATH):
        return {}
    try:
        with open(_CFG_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}

def _save_cfg_all(all_cfg: Dict[str, Any]) -> None:
    _ensure_cfg_dir()
    tmp = _CFG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(all_cfg, f, ensure_ascii=False, indent=2)
    os.replace(tmp, _CFG_PATH)

def _get_cfg(site_id: str) -> Optional[Dict[str, Any]]:
    all_cfg = _load_cfg_all()
    v = all_cfg.get(site_id)
    return v if isinstance(v, dict) else None

def _set_cfg(site_id: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    all_cfg = _load_cfg_all()
    all_cfg[site_id] = cfg
    _save_cfg_all(all_cfg)
    return cfg

def _pv_est_kw(ghi_wm2: Optional[float], pv_kwp: float, pr: float = 0.85) -> Optional[float]:
    # stima semplice: P(kW) = kWp * (GHI/1000) * PR
    if ghi_wm2 is None:
        return None
    try:
        return float(pv_kwp) * (float(ghi_wm2) / 1000.0) * float(pr)
    except Exception:
        return None

@router.get("/site/{site_id}/weather_cfg")
def get_weather_cfg(site_id: str) -> Dict[str, Any]:
    cfg = _get_cfg(site_id)
    return {"site_id": site_id, "cfg": cfg}

@router.put("/site/{site_id}/weather_cfg")
def put_weather_cfg(
    site_id: str,
    lat: float = Query(...),
    lon: float = Query(...),
    pv_kwp: float = Query(10.0, description="Potenza di picco del campo FV (kWp)"),
    timezone: str = Query("UTC"),
    pr: float = Query(0.85, description="Performance ratio (0..1), default 0.85"),
) -> Dict[str, Any]:
    cfg = {
        "lat": float(lat),
        "lon": float(lon),
        "pv_kwp": float(pv_kwp),
        "timezone": str(timezone),
        "pr": float(pr),
    }
    _set_cfg(site_id, cfg)
    return {"site_id": site_id, "cfg": cfg}

@router.get("/site/{site_id}/weather")
async def site_weather(
    site_id: str,
    # opzionali: se non passati, usa cfg salvata
    lat: Optional[float] = Query(None),
    lon: Optional[float] = Query(None),
    pv_kwp: Optional[float] = Query(None, description="Potenza di picco del campo FV (kWp)"),
    timezone: Optional[str] = Query(None),
    pr: Optional[float] = Query(None, description="Performance ratio (0..1), default 0.85"),
) -> Dict[str, Any]:
    """
    Open-Meteo:
    - current: temp, wind, cloud cover, ecc.
    - hourly: shortwave_radiation (GHI) per stimare potenza FV
    Restituisce anche stima produzione per 72h (3 giorni) su base oraria + aggregazione giornaliera.
    """

    saved = _get_cfg(site_id) or {}
    lat = lat if lat is not None else saved.get("lat")
    lon = lon if lon is not None else saved.get("lon")
    pv_kwp = pv_kwp if pv_kwp is not None else saved.get("pv_kwp", 10.0)
    timezone = timezone if timezone is not None else saved.get("timezone", "UTC")
    pr = pr if pr is not None else saved.get("pr", 0.85)

    if lat is None or lon is None:
        raise HTTPException(status_code=400, detail="Missing lat/lon. Set /weather_cfg once, or pass lat/lon query.")

    url = "https://api.open-meteo.com/v1/forecast"

    hourly = [
        "temperature_2m",
        "cloud_cover",
        "precipitation",
        "wind_speed_10m",
        "shortwave_radiation",
        "direct_radiation",
        "diffuse_radiation",
        "uv_index",
    ]
    current = [
        "temperature_2m",
        "wind_speed_10m",
        "cloud_cover",
        "precipitation",
        "uv_index",
    ]

    params = {
        "latitude": lat,
        "longitude": lon,
        "timezone": timezone,
        "forecast_days": 3,
        "hourly": ",".join(hourly),
        "current": ",".join(current),
    }

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        j = r.json()

    # Stima produzione oraria dai valori di shortwave_radiation (W/m2)
    ht = (j.get("hourly", {}) or {}).get("time", []) or []
    ghi = (j.get("hourly", {}) or {}).get("shortwave_radiation", []) or []

    production_est_kw: List[Optional[float]] = []
    for x in ghi:
        production_est_kw.append(_pv_est_kw(x, pv_kwp=float(pv_kwp), pr=float(pr)))

    # Aggregazione giornaliera (kWh) semplice: somma(kW)*1h
    daily_kwh: Dict[str, float] = {}
    for t, pkw in zip(ht, production_est_kw):
        if not t or pkw is None:
            continue
        day = str(t)[:10]
        daily_kwh[day] = daily_kwh.get(day, 0.0) + float(pkw) * 1.0

    return {
        "site_id": site_id,
        "source": "open-meteo",
        "lat": lat,
        "lon": lon,
        "timezone": j.get("timezone", timezone),
        "pv_kwp": float(pv_kwp),
        "pr": float(pr),
        "current": j.get("current", {}),
        "hourly": j.get("hourly", {}),
        "production_est_kw": {
            "time": ht,
            "p_kw": production_est_kw,
            "daily_kwh": daily_kwh,
        },
    }
