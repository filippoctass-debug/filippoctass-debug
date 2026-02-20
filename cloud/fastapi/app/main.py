from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.middleware_fix import FixObjectObjectSiteMiddleware
from app.modbus.router import router as edge_router
from app.routes import router as router_v1
from app.mqtt_ingest import start_ingest
from app.timeseries_api import router as ts_router
from app.pv_aggregate_api import router as pv_router
from app.ws import websocket_endpoint

app = FastAPI(title="Control Room API")

# Se usi davvero questo middleware, assicurati di aggiungerlo (nel tuo snippet non era usato)
app.add_middleware(FixObjectObjectSiteMiddleware)

# Mettiamo TUTTE le route REST sotto /api
app.include_router(edge_router, prefix="/api")
app.include_router(router_v1, prefix="/api")     # <-- FIX: ora /api/sites esiste
app.include_router(ts_router, prefix="/api")
app.include_router(pv_router, prefix="/api")

app.mount("/static", StaticFiles(directory="app/static"), name="static")

@app.on_event("startup")
def _startup():
    start_ingest()

app.add_api_websocket_route("/ws/status", websocket_endpoint)
