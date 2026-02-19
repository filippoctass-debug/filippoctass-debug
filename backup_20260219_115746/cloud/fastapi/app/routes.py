from fastapi import APIRouter

from app.devices_api import router as devices_router

# OPTIONAL routers (safe import)
try:
    from app.device_data_api import router as device_data_router
except Exception:
    device_data_router = None

try:
    from app.weather_api import router as weather_router
except Exception:
    weather_router = None


router = APIRouter()

# existing
router.include_router(devices_router)

# new: inverter data (Influx)
if device_data_router is not None:
    router.include_router(device_data_router, prefix="/api")

# new: weather (Open-Meteo)
if weather_router is not None:
    router.include_router(weather_router, prefix="/api")
