import asyncio
from fastapi import WebSocket, WebSocketDisconnect

async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            # heartbeat ogni 2s
            await ws.send_json({"type": "heartbeat"})
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        return
