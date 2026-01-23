import os
from app.influx import get_influx_client

def list_sites(lookback_days:int=7):
    org=os.getenv("INFLUX_ORG","")
    bucket=os.getenv("INFLUX_BUCKET","")
    c=get_influx_client()
    flux=f'''
from(bucket: "{bucket}")
  |> range(start: -{lookback_days}d)
  |> filter(fn: (r) => exists r.site_id)
  |> keep(columns: ["site_id"])
  |> group()
  |> distinct(column: "site_id")
'''
    tables=c.query_api().query(flux, org=org)
    out=[]
    for t in tables:
        for r in t.records:
            sid=r.get_value()
            if sid and sid not in out: out.append(sid)
    return sorted(out)
