import json
import re
from typing import Optional
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

OBJ_RE = re.compile(r"/api/site/\[object%20Object\]/", re.IGNORECASE)

def _pick_site_id_from_query(request: Request) -> Optional[str]:
    # prova: ?site_id=PV_002
    sid = request.query_params.get("site_id")
    if sid:
        return sid

    # prova: ?site={"site_id":"PV_002", ...}
    s = request.query_params.get("site")
    if s:
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and obj.get("site_id"):
                return str(obj["site_id"])
        except Exception:
            pass
    return None

class FixObjectObjectSiteMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.scope.get("path", "")

        if "[object Object]" in path or "%5Bobject%20Object%5D" in request.url.path.lower():
            sid = _pick_site_id_from_query(request)
            if not sid:
                # fallback: prova a prendere PV_002 o PV_001 (ordine di preferenza)
                # (non importiamo app.sites qui per evitare cicli)
                sid = "PV_002"

            new_path = OBJ_RE.sub(f"/api/site/{sid}/", request.url.path)
            request.scope["path"] = new_path

        return await call_next(request)
