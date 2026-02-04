from fastapi import FastAPI
from app.modbus.router import router as edge_router
from fastapi.staticfiles import StaticFiles
from app.routes import router
from app.mqtt_ingest import start_ingest
from app.timeseries_api import router as ts_router
from app.ingest import start_ingest

app = FastAPI(title="Control Room API")

app.include_router(edge_router)
app.include_router(router)
app.include_router(ts_router, prefix="/api")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

@app.on_event("startup")
def _startup():
    start_ingest()

