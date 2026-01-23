from fastapi import APIRouter
from app.status import get_control_room_status
from app.sites import list_sites
from app.ws import ws_router

router=APIRouter()

@router.get('/')
def root():
    return {'ok': True, 'ui': '/static/index.html'}

@router.get('/sites')
def sites(lookback_days:int=7):
    return {'sites': list_sites(lookback_days=lookback_days)}

@router.get('/control-room/status')
def control_room_status(lookback_minutes:int=10):
    return get_control_room_status(lookback_minutes=lookback_minutes)

router.include_router(ws_router)
