from fastapi import APIRouter, HTTPException, Query
from app.sites import list_sites, delete_site

router = APIRouter()

@router.get("/sites")
def sites(
    lookback_days: int = Query(7, ge=1, le=365),
    online_within_seconds: int = Query(120, ge=5, le=3600),
):
    return {"sites": list_sites(lookback_days=lookback_days, online_within_seconds=online_within_seconds)}

@router.get("/site_ids")
def site_ids(
    lookback_days: int = Query(30, ge=1, le=365),
):
    items = list_sites(lookback_days=lookback_days, online_within_seconds=999999)
    return {"sites": [str(x.get("site_id")) for x in items if x.get("site_id")]}

@router.delete("/sites/{site_id}")
def sites_delete(site_id: str):
    ok = delete_site(site_id)
    if not ok:
        raise HTTPException(status_code=404, detail="site_id not found")
    return {"ok": True, "deleted": site_id}
