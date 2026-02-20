import os
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from fastapi import APIRouter, Query
from influxdb_client import InfluxDBClient

from app.influx import get_influx_client

router = APIRouter(tags=["pv-aggregate"])

INFLUX_ORG = os.getenv("INFLUX_ORG", os.getenv("INFLUXD_INIT_ORG", "pv"))
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", os.getenv("INFLUXD_INIT_BUCKET", "cci"))

MEASUREMENT = os.getenv("INFLUX_MEASUREMENT_TELEMETRY", "pv_telemetry_v2")
TAG_SITE = os.getenv("INFLUX_TAG_SITE", "site_id")

# IMPORTANT: nel tuo payload MQTT i tag sono es. "device":"KACO_1"
TAG_DEVICE = os.getenv("INFLUX_TAG_DEVICE_ID", "device")


def _registry_path() -> Path:
    # /app/app/storage in container, in repo è cloud/fastapi/app/storage
    here = Path(__file__).resolve().parent
    return here / "storage" / "devices_registry.json"


def _read_registry() -> Dict[str, Any]:
    p = _registry_path()
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


def _get_cfg_devices(site_id: str) -> List[Dict[str, Any]]:
    reg = _read_registry()
    site = (reg.get("sites") or {}).get(str(site_id)) or {}
    devs = site.get("devices") or []
    out: List[Dict[str, Any]] = []
    for d in devs:
        # enabled default True
        if (d.get("enabled") is None) or bool(d.get("enabled")):
            out.append(d)
    return out


def _map_to_influx_device_tags(site_id: str) -> List[str]:
    """
    Ritorna i valori da usare nel tag Influx TAG_DEVICE (di default "device").

    Dal tuo mosquitto_sub:
      tags: {"device":"KACO_1", "unit_id":1, ...}
    quindi il match corretto è sui NOMI (KACO_1, KACO_2, ...).

    Regole:
    - se in registry c'è "name" usa quello (es. KACO_1)
    - fallback: "id"
    - fallback estremo: dev{i+1}
    Inoltre, per robustezza, includiamo sia name che id se entrambi presenti.
    """
    devs = _get_cfg_devices(site_id)

    out: List[str] = []
    seen: Set[str] = set()

    for i, d in enumerate(devs):
        candidates: List[str] = []

        nm = str(d.get("name") or "").strip()
        did = str(d.get("id") or "").strip()

        if nm:
            candidates.append(nm)
        if did and did != nm:
            candidates.append(did)

        if not candidates:
            candidates.append(f"dev{i+1}")

        for c in candidates:
            if c and c not in seen:
                seen.add(c)
                out.append(c)

    return out


@router.get("/site/{site_id}/pv/latest")
def pv_latest(site_id: str, lookback_s: int = Query(3600, ge=30, le=86400)) -> Dict[str, Any]:
    device_tags = _map_to_influx_device_tags(site_id)
    if not device_tags:
        return {
            "site_id": site_id,
            "pac_sum_w": None,
            "devices": [],
            "device_tag": TAG_DEVICE,
            "note": "no devices configured/enabled",
        }

    ids_flux = "[" + ",".join([f'"{x}"' for x in device_tags]) + "]"

    # last() per device sull'ultimo valore di p_ac_w
    flux = f"""
ids = {ids_flux}
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{lookback_s}s)
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")
  |> filter(fn: (r) => r["{TAG_SITE}"] == "{site_id}")
  |> filter(fn: (r) => exists r["{TAG_DEVICE}"])
  |> filter(fn: (r) => contains(value: r["{TAG_DEVICE}"], set: ids))
  |> filter(fn: (r) => r._field == "p_ac_w")
  |> group(columns: ["{TAG_DEVICE}"])
  |> last()
""".strip()

    c: InfluxDBClient = get_influx_client()
    tables = c.query_api().query(flux, org=INFLUX_ORG)

    dev_rows: List[Dict[str, Any]] = []
    pac_sum = 0.0
    has_any = False

    for t in tables or []:
        for r in t.records:
            did = r.values.get(TAG_DEVICE)
            ts = r.get_time()
            v = r.get_value()
            pac = float(v) if isinstance(v, (int, float)) else None

            dev_rows.append(
                {
                    "device": did,
                    "last_ts": ts.isoformat() if ts else None,
                    "p_ac_w": pac,
                }
            )

            if pac is not None:
                pac_sum += pac
                has_any = True

    # ordina per device per stabilità UI
    dev_rows.sort(key=lambda x: str(x.get("device") or ""))

    return {
        "site_id": site_id,
        "pac_sum_w": (pac_sum if has_any else None),
        "devices": dev_rows,
        "device_tag": TAG_DEVICE,
        "cfg_devices_enabled": len(_get_cfg_devices(site_id)),
        "influx_device_values": device_tags,
        "lookback_s": lookback_s,
    }


@router.get("/site/{site_id}/pv/series")
def pv_series(
    site_id: str,
    minutes: int = Query(120, ge=5, le=7 * 24 * 60),
    every: str = Query("10s"),
) -> Dict[str, Any]:
    device_tags = _map_to_influx_device_tags(site_id)
    if not device_tags:
        return {
            "site_id": site_id,
            "t": [],
            "p_ac_w": [],
            "minutes": minutes,
            "every": every,
            "device_tag": TAG_DEVICE,
            "note": "no devices configured/enabled",
        }

    ids_flux = "[" + ",".join([f'"{x}"' for x in device_tags]) + "]"

    # serie aggregata: mean per device nella finestra, poi somma su tutti i device per timestamp
    flux = f"""
ids = {ids_flux}
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{minutes}m)
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}")
  |> filter(fn: (r) => r["{TAG_SITE}"] == "{site_id}")
  |> filter(fn: (r) => exists r["{TAG_DEVICE}"])
  |> filter(fn: (r) => contains(value: r["{TAG_DEVICE}"], set: ids))
  |> filter(fn: (r) => r._field == "p_ac_w")
  |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
  |> group(columns: ["_time"])
  |> sum(column: "_value")
  |> keep(columns: ["_time", "_value"])
  |> sort(columns: ["_time"])
""".strip()

    c: InfluxDBClient = get_influx_client()
    tables = c.query_api().query(flux, org=INFLUX_ORG)

    out_t: List[str] = []
    out_p: List[Optional[float]] = []

    for tb in tables or []:
        for rec in tb.records:
            ts = rec.get_time()
            if not ts:
                continue
            out_t.append(ts.isoformat().replace("+00:00", "Z"))
            v = rec.get_value()
            out_p.append(float(v) if isinstance(v, (int, float)) else None)

    return {
        "site_id": site_id,
        "t": out_t,
        "p_ac_w": out_p,
        "minutes": minutes,
        "every": every,
        "device_tag": TAG_DEVICE,
        "cfg_devices_enabled": len(_get_cfg_devices(site_id)),
        "influx_device_values": device_tags,
    }