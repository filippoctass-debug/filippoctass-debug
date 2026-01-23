import os
from fastapi import APIRouter
from app.influx import get_influx_client

router = APIRouter()

@router.get("/site/{site_id}/series")
def site_series(site_id: str, minutes: int = 120, every: str = "10s"):
    """
    Ritorna serie recenti per un impianto:
    - p_ac_w
    - poa_wm2
    """
    org = os.getenv("INFLUX_ORG", "")
    bucket = os.getenv("INFLUX_BUCKET", "")
    c = get_influx_client()

    flux = f'''
from(bucket: "{bucket}")
  |> range(start: -{minutes}m)
  |> filter(fn: (r) => r._measurement == "pv_telemetry")
  |> filter(fn: (r) => r.site_id == "{site_id}")
  |> filter(fn: (r) => r._field == "p_ac_w" or r._field == "poa_wm2")
  |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
  |> keep(columns: ["_time","_field","_value"])
'''
    tables = c.query_api().query(flux, org=org)

    # output strutturato per frontend
    out = {"site_id": site_id, "t": [], "p_ac_w": [], "poa_wm2": []}
    rows = []
    for t in tables:
        for r in t.records:
            rows.append((r.get_time(), r.get_field(), r.get_value()))

    # ordina per tempo
    rows.sort(key=lambda x: x[0])

    # ricostruisci timeline (due serie con stesso timestamp)
    last_t = None
    cur = {"p_ac_w": None, "poa_wm2": None}
    for ts, field, val in rows:
        if last_t is None:
            last_t = ts
        if ts != last_t:
            out["t"].append(last_t.isoformat())
            out["p_ac_w"].append(cur["p_ac_w"])
            out["poa_wm2"].append(cur["poa_wm2"])
            cur = {"p_ac_w": None, "poa_wm2": None}
            last_t = ts
        if field in cur:
            cur[field] = float(val) if val is not None else None

    if last_t is not None:
        out["t"].append(last_t.isoformat())
        out["p_ac_w"].append(cur["p_ac_w"])
        out["poa_wm2"].append(cur["poa_wm2"])

    return out
