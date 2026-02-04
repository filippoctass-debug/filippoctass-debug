import os
from fastapi import APIRouter
from app.influx import get_influx_client

router = APIRouter()


@router.get("/site/{site_id}/series")
def site_series(site_id: str, minutes: int = 120, every: str = "10s"):
    """
    Returns downsampled series for a site:
      - t: ISO timestamps
      - p_ac_w
      - poa_wm2
    Uses Flux pivot() so fields come back as columns (no _field in records).
    """

    org = os.getenv("INFLUX_ORG", "")
    bucket = os.getenv("INFLUX_BUCKET", "")
    c = get_influx_client()

    # Note: pivot() removes _field/_value and turns them into columns
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
                # skip malformed rows
                continue

            out["t"].append(t.isoformat())
            out["p_ac_w"].append(record.values.get("p_ac_w"))
            out["poa_wm2"].append(record.values.get("poa_wm2"))

    return out
