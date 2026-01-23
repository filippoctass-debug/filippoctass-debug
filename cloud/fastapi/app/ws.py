import asyncio
from fastapi import APIRouter, WebSocket
from app.status import get_control_room_status

ws_router=APIRouter()

@ws_router.websocket("/ws/status")
async def ws_status(ws:WebSocket):
    await ws.accept()
    try:
        while True:
            await ws.send_json(get_control_room_status(lookback_minutes=10))
            await asyncio.sleep(2)
    except Exception:
        pass
