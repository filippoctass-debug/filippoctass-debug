import asyncio
import json
import os
import time
from typing import List, Dict, Any

from fastapi import WebSocket


def _ws_get_sites_status() -> List[Dict[str, Any]]:
    """
    Ritorna lista siti dedotta da Influx: ultimi punti per site_id nel measurement telemetry.
    Richiede tag: site_id.
    """
    try:
        from influxdb_client import InfluxDBClient as C

        url = os.getenv("INFLUX_URL")
        token = os.getenv("INFLUX_TOKEN")
        org = os.getenv("INFLUX_ORG")
        bucket = os.getenv("INFLUX_BUCKET")
        meas = os.getenv("INFLUX_MEASUREMENT_TELEMETRY", "pv_telemetry_v2")

        if not (url and token and org and bucket):
            return []

        q = f'''
from(bucket: "{bucket}")
  |> range(start: -10m)
  |> filter(fn: (r) => r._measurement == "{meas}")
  |> group(columns: ["site_id"])
  |> last()
  |> keep(columns: ["_time","site_id"])
'''

        sites = []
        seen = set()

        with C(url=url, token=token, org=org) as c:
            tables = c.query_api().query(q)
            for t in tables:
                for r in t.records:
                    sid = str(dict(r.values).get("site_id") or "")
                    if not sid or sid in seen:
                        continue
                    seen.add(sid)
                    ts = r.get_time()
                    sites.append({
                        "site_id": sid,
                        "ok": True,
                        "last_ts": ts.isoformat().replace("+00:00", "Z") if ts else None
                    })

        sites.sort(key=lambda x: x["site_id"])
        return sites
    except Exception:
        return []


async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    last_status = 0.0
    while True:
        try:
            # heartbeat
            await websocket.send_text(json.dumps({"type": "heartbeat"}))

            # status (sites) ogni 2 secondi
            now = time.time()
            if (now - last_status) >= 2.0:
                sites = _ws_get_sites_status()
                await websocket.send_text(json.dumps({"type": "status", "sites": sites}))
                last_status = now

            await asyncio.sleep(1.0)

        except Exception:
            break

