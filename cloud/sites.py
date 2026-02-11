from app.influx import get_influx_client
import os

def list_sites(lookback_days: int = 2) -> list[str]:
    """
    Ritorna l'elenco dei site_id presenti nel bucket Influx.
    lookback_days: quanti giorni indietro cercare (default 2).
    """
    org = os.getenv("INFLUX_ORG", "")
    bucket = os.getenv("INFLUX_BUCKET", "")
    c = get_influx_client()

    # Flux range: usa giorni, così è coerente con lookback_days
    flux = f"""
from(bucket:"{bucket}")
  |> range(start:-{int(lookback_days)}d)
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
