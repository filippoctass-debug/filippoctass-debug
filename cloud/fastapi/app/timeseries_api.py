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
  |> filter(fn: (r) => r._measurement == "pv_telemetry")
  |> filter(fn: (r) => r.site_id == "{site_id}")
  |> filter(fn: (r) => r._field == "p_ac_w" or r._field == "poa_wm2")
  |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
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
