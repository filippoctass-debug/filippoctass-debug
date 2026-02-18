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

    Nota: usiamo pivot() -> i campi diventano colonne ("p_ac_w", "poa_wm2"),
    quindi NON esistono più _field/_value nei record.
    """

    org = os.getenv("INFLUX_ORG", "")
    bucket = os.getenv("INFLUX_BUCKET", "")
    c = get_influx_client()

    flux = f"""
from(bucket: "{bucket}")
  |> range(start: -{minutes}m)
  |> filter(fn: (r) => r._measurement == "pv_telemetry_v2")
  |> filter(fn: (r) => r.site_id == "{site_id}")
  |> filter(fn: (r) => r._field == "p_ac_w" or r._field == "poa_wm2")
    |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
  |> group(columns: ["_time","_field"])
  |> sum(column: "_value")
  |> sort(columns: ["_time"])  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "p_ac_w", "poa_wm2"])
""".strip()

    tables = c.query_api().query(flux, org=org)

    out = {"site_id": site_id, "t": [], "p_ac_w": [], "poa_wm2": []}

    for table in tables:
        for record in table.records:
            t = record.get_time()
            if t is None:
                continue
            out["t"].append(t.isoformat())
            out["p_ac_w"].append(record.values.get("p_ac_w"))
            out["poa_wm2"].append(record.values.get("poa_wm2"))

    return out


def _map_device_fallback(site_id: str, device_id: str):
    """
    Se non esistono dati per device_id,
    prova mapping automatico inv_N -> devN
    basato sull'ordine nel config site.
    """
    try:
        from app.storage.edge_config import get_site_config
        cfg = get_site_config(site_id)
        devices = cfg.get("devices", [])
        for i,d in enumerate(devices):
            if str(d.get("id")) == device_id:
                return f"dev{i+1}"
    except Exception:
        pass
    return None



