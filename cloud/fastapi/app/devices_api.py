from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field


router = APIRouter(prefix="/api", tags=["devices"])

_LOCK = threading.Lock()


def _storage_path() -> Path:
    # /app/app/storage in container, in repo è cloud/fastapi/app/storage
    here = Path(__file__).resolve().parent
    return here / "storage" / "devices_registry.json"


def _read_registry() -> Dict[str, Any]:
    p = _storage_path()
    if not p.exists():
        return {"version": 1, "sites": {}}
    try:
        raw = p.read_text(encoding="utf-8", errors="ignore").strip()
        if not raw:
            return {"version": 1, "sites": {}}
        data = json.loads(raw)
        if not isinstance(data, dict):
            return {"version": 1, "sites": {}}
        data.setdefault("version", 1)
        data.setdefault("sites", {})
        if not isinstance(data["sites"], dict):
            data["sites"] = {}
        return data
    except Exception:
        # non facciamo crashare tutto per un json sporco
        return {"version": 1, "sites": {}}


def _atomic_write(p: Path, data: Dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    # scrittura atomica (temp + replace)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", newline="\n") as f:
        f.write(payload)
        tmp = f.name
    os.replace(tmp, str(p))


class ModbusDevice(BaseModel):
    # id interno (stringa) che useremo anche per setpoint target
    id: str = Field(..., min_length=1, max_length=64)
    name: str = Field("inverter", max_length=128)

    host: str = Field(..., min_length=1, max_length=255)  # ip/dns
    port: int = Field(502, ge=1, le=65535)
    unit_id: int = Field(1, ge=0, le=247)

    enabled: bool = True

    # per ora fissiamo Sunspec (poi estendiamo)
    model: str = Field("sunspec", max_length=32)


class DevicesPayload(BaseModel):
    site_id: str = Field(..., min_length=1, max_length=64)
    devices: List[ModbusDevice] = Field(default_factory=list)


def _normalize_site_id(site_id: str) -> str:
    return str(site_id).strip()


@router.get("/site/{site_id}/devices")
def get_devices(site_id: str):
    sid = _normalize_site_id(site_id)
    with _LOCK:
        reg = _read_registry()
        site = reg["sites"].get(sid) or {"devices": []}
        devices = site.get("devices") or []
    return {"site_id": sid, "devices": devices}


@router.put("/site/{site_id}/devices")
def put_devices(site_id: str, payload: DevicesPayload):
    sid = _normalize_site_id(site_id)
    if _normalize_site_id(payload.site_id) != sid:
        raise HTTPException(status_code=400, detail="site_id mismatch")

    # normalizzazione e dedup id
    devices_out: List[Dict[str, Any]] = []
    seen = set()
    for d in payload.devices:
        did = str(d.id).strip()
        if not did:
            raise HTTPException(status_code=400, detail="device id cannot be empty")
        if did in seen:
            raise HTTPException(status_code=400, detail=f"duplicate device id: {did}")
        seen.add(did)
        devices_out.append(d.model_dump())

    with _LOCK:
        reg = _read_registry()
        reg.setdefault("version", 1)
        reg.setdefault("sites", {})
        reg["sites"][sid] = {"devices": devices_out}
        _atomic_write(_storage_path(), reg)

    return {"site_id": sid, "devices": devices_out, "saved": True}
