from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.storage.edge_config import get_site_config, set_site_config

router = APIRouter(prefix="/api/edge", tags=["edge"])

class Device(BaseModel):
    name: str = Field(default="dev1")
    host: str = Field(..., description="IP address or hostname")
    port: int = Field(default=502, ge=1, le=65535)
    unit_id: int = Field(default=1, ge=0, le=255)

class SiteConfig(BaseModel):
    site_id: str
    devices: List[Device] = Field(default_factory=list)

@router.get("/{site_id}", response_model=SiteConfig)
def api_get(site_id: str):
    cfg = get_site_config(site_id)
    return cfg

@router.put("/{site_id}", response_model=SiteConfig)
def api_put(site_id: str, body: SiteConfig):
    if body.site_id != site_id:
        raise HTTPException(status_code=400, detail="site_id mismatch")
    cfg = set_site_config(site_id, body.model_dump())
    return cfg

@router.post("/{site_id}/devices", response_model=SiteConfig)
def api_add_device(site_id: str, dev: Device):
    cfg = get_site_config(site_id)
    cfg["site_id"] = site_id
    cfg.setdefault("devices", [])
    cfg["devices"].append(dev.model_dump())
    cfg = set_site_config(site_id, cfg)
    return cfg

@router.delete("/{site_id}/devices/{idx}", response_model=SiteConfig)
def api_del_device(site_id: str, idx: int):
    cfg = get_site_config(site_id)
    devices = cfg.get("devices", [])
    if idx < 0 or idx >= len(devices):
        raise HTTPException(status_code=404, detail="device index not found")
    devices.pop(idx)
    cfg["devices"] = devices
    cfg = set_site_config(site_id, cfg)
    return cfg
