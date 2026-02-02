from __future__ import annotations

import uuid
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List

from .models import ModbusConfig, ModbusDevice
from .storage import load_config, save_config

router = APIRouter(prefix="/api/modbus", tags=["modbus-config"])


class ModbusConfigUpdate(BaseModel):
    site_id: str = Field(..., description="Site/Edge identifier")
    devices: List[ModbusDevice]


@router.get("/sites/{site_id}", response_model=ModbusConfig)
def get_site_modbus(site_id: str):
    return load_config(site_id)


@router.put("/sites/{site_id}", response_model=ModbusConfig)
def put_site_modbus(site_id: str, payload: ModbusConfigUpdate):
    if payload.site_id != site_id:
        raise HTTPException(status_code=400, detail="site_id mismatch")
    cfg = ModbusConfig(site_id=site_id, devices=payload.devices)
    return save_config(cfg)


@router.post("/sites/{site_id}/device", response_model=ModbusConfig)
def add_device(site_id: str, device: ModbusDevice):
    cfg = load_config(site_id)
    # se id vuoto o duplicato, genera uuid
    ids = {d.id for d in cfg.devices}
    if not device.id or device.id in ids:
        device.id = str(uuid.uuid4())
    cfg.devices.append(device)
    return save_config(cfg)


@router.delete("/sites/{site_id}/device/{device_id}", response_model=ModbusConfig)
def delete_device(site_id: str, device_id: str):
    cfg = load_config(site_id)
    cfg.devices = [d for d in cfg.devices if d.id != device_id]
    return save_config(cfg)


@router.get("/sites/{site_id}/edge-env", response_model=dict)
def get_edge_env(site_id: str):
    cfg = load_config(site_id)
    return {
        "site_id": site_id,
        "MODBUS_TARGETS": cfg.to_modbus_targets_env(),
        "devices_count": len([d for d in cfg.devices if d.enabled]),
    }
