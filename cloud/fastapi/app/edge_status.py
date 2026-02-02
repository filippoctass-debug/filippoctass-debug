import os
from datetime import datetime, timezone
from app.influx import get_influx_client

def get_edge_status(lookback_minutes:int=60):
    org=os.getenv("INFLUX_ORG","")
    bucket=os.getenv("INFLUX_BUCKET","")
    max_age=int(os.getenv("STATUS_MAX_AGE_SECONDS","180"))
    c=get_influx_client()
    flux=f'''
from(bucket: "{bucket}")
  |> range(start: -{lookback_minutes}m)
  |> filter(fn: (r) => r._measurement == "edge_status")
  |> last()
  |> group(columns: ["site_id"])
'''
    tables=c.query_api().query(flux, org=org)
    now=datetime.now(timezone.utc)
    sites={}
    for t in tables:
        for r in t.records:
            sid=r.values.get("site_id")
            if not sid: 
                continue
            ts=r.get_time()
            age=(now-ts).total_seconds() if ts else 1e9
            sites.setdefault(sid, {"site_id":sid,"ok":False,"age_s":None,"last_ts":None,"fields":{}})
            sites[sid]["age_s"]=int(age)
            sites[sid]["last_ts"]=ts.isoformat() if ts else None
            sites[sid]["fields"][r.get_field()]=r.get_value()
            sites[sid]["ok"]=age<=max_age
    return {"generated_at": now.isoformat(), "sites": list(sites.values())}
