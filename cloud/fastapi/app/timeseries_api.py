import os
from fastapi import APIRouter
from app.influx import get_influx_client

router = APIRouter()


@router.get("/site/{site_id}/series")
def site_series(site_id: str, minutes: int = 120, every: str = "10s"):

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
      |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
      |> keep(columns: ["_time","p_ac_w","poa_wm2"])
    '''

    tables = c.query_api().query(flux, org=org)

    out = {"site_id": site_id, "t": [], "p_ac_w": [], "poa_wm2": []}

    for table in tables:
        for record in table.records:
            out["t"].append(record.get_time().isoformat())
            out["p_ac_w"].append(record.values.get("p_ac_w"))
            out["poa_wm2"].append(record.values.get("poa_wm2"))

    return out
