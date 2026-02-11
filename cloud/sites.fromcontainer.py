from app.influx import get_influx_client
import os

def list_sites() -> list[str]:
    org = os.getenv("INFLUX_ORG", "")
    bucket = os.getenv("INFLUX_BUCKET", "")
    c = get_influx_client()

    flux = f"""
from(bucket:"{bucket}")
  |> range(start:-2h)
  |> keep(columns:["site_id"])
  |> distinct(column:"site_id")
"""
    tables = c.query_api().query(flux, org=org)

    out = []
    for t in tables:
        for r in t.records:
            sid = r.values.get("site_id")
            if sid and str(sid) not in out:
                out.append(str(sid))
    out.sort()
    return out
