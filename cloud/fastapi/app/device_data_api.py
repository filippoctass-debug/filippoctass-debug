import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from influxdb_client import InfluxDBClient

router = APIRouter(tags=["device-data"])

INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG = os.getenv("INFLUX_ORG", os.getenv("INFLUXD_INIT_ORG", "pv"))
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", os.getenv("INFLUXD_INIT_BUCKET", "cci"))

MEASUREMENT = os.getenv("INFLUX_MEASUREMENT_TELEMETRY", "pv_telemetry_v2")
TAG_SITE = os.getenv("INFLUX_TAG_SITE", "site_id")

# IMPORTANT: nel tuo Influx sembra esserci tag "device" (es: dev1).
# Se nel docker-compose hai già INFLUX_TAG_DEVICE_ID=device va bene.
TAG_DEVICE_ID = os.getenv("INFLUX_TAG_DEVICE_ID", "device")


def _client() -> InfluxDBClient:
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)


def _map_device_fallback(site_id: str, device_id: str) -> str:
    """
    Mappa l'id configurato (es inv_1) all'id usato in Influx (es dev1),
    basandosi sull'ordine dei devices nel config del sito.
    Se non riesce, ritorna device_id com'è.
    """
    try:
        if isinstance(device_id, str) and device_id.startswith("dev"):
            return device_id

        from app.storage.edge_config import get_site_config
        cfg = get_site_config(site_id) or {}
        devs = cfg.get("devices", []) or []
        for i, d in enumerate(devs):
            if str(d.get("id", "")) == str(device_id):
                return f"dev{i+1}"
    except Exception:
        pass

    return device_id


@router.get("/site/{site_id}/device/{device_id}/latest")
def device_latest(site_id: str, device_id: str, lookback_s: int = Query(3600, ge=30, le=86400)) -> Dict[str, Any]:
    """
    Ultimo punto per un inverter (device_id) nel measurement pv_telemetry_v2.
    Richiede che in Influx i punti abbiano tag site_id e TAG_DEVICE_ID (di default device_id, spesso "device").
    """
    req_device_id = device_id
    influx_device_id = _map_device_fallback(site_id, device_id)

    flux = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{lookback_s}s)
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")
  |> filter(fn: (r) => r["{TAG_SITE}"] == "{site_id}")
  |> filter(fn: (r) => r["{TAG_DEVICE_ID}"] == "{influx_device_id}")
  |> last()
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "{TAG_SITE}", "{TAG_DEVICE_ID}", "p_ac_w", "grid_v", "freq_hz", "i_ac_a", "v_dc_v", "manufacturer", "model", "serial", "source"])
'''.strip()

    with _client() as c:
        tables = c.query_api().query(flux)

    if not tables or not tables[0].records:
        return {
            "site_id": site_id,
            "device_id": req_device_id,
            "ok": None,
            "last_ts": None,
            "fields": {},
        }

    rec = tables[0].records[0]
    fields = rec.values.copy()
    last_ts = fields.pop("_time", None)

    # ripulisci meta influx
    for k in list(fields.keys()):
        if k.startswith("_") or k in ("result", "table"):
            fields.pop(k, None)

    return {
        "site_id": site_id,
        "device_id": req_device_id,
        "ok": True,
        "last_ts": last_ts.isoformat() if last_ts else None,
        "fields": fields,
    }


@router.get("/site/{site_id}/device/{device_id}/series")
def device_series(
    site_id: str,
    device_id: str,
    minutes: int = Query(120, ge=5, le=7*24*60),
    every: str = Query("10s"),
) -> Dict[str, Any]:
    """
    Serie per inverter: p_ac_w + (se presenti) grid_v/freq_hz/v_dc_v.
    """
    req_device_id = device_id
    influx_device_id = _map_device_fallback(site_id, device_id)

    flux = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{minutes}m)
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")
  |> filter(fn: (r) => r["{TAG_SITE}"] == "{site_id}")
  |> filter(fn: (r) => r["{TAG_DEVICE_ID}"] == "{influx_device_id}")
  |> filter(fn: (r) => r._field == "p_ac_w" or r._field == "grid_v" or r._field == "freq_hz" or r._field == "v_dc_v")
  |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "p_ac_w", "grid_v", "freq_hz", "v_dc_v"])
'''.strip()

    with _client() as c:
        tables = c.query_api().query(flux)

    t: List[str] = []
    p_ac_w: List[Optional[float]] = []
    grid_v: List[Optional[float]] = []
    freq_hz: List[Optional[float]] = []
    v_dc_v: List[Optional[float]] = []

    if tables:
        for rec in tables[0].records:
            ts = rec.get_time()
            t.append(ts.isoformat() if ts else "")
            p_ac_w.append(rec.values.get("p_ac_w"))
            grid_v.append(rec.values.get("grid_v"))
            freq_hz.append(rec.values.get("freq_hz"))
            v_dc_v.append(rec.values.get("v_dc_v"))

    return {
        "site_id": site_id,
        "device_id": req_device_id,
        "t": t,
        "p_ac_w": p_ac_w,
        "grid_v": grid_v,
        "freq_hz": freq_hz,
        "v_dc_v": v_dc_v,
        "every": every,
        "minutes": minutes,
    }



