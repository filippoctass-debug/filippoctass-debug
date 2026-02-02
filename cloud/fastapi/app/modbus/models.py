from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, Field, conint


class ModbusDevice(BaseModel):
    id: str = Field(..., description="Stable device id (uuid or slug).")
    name: str = Field(..., description="Human readable name.")
    host: str = Field(..., description="IP or hostname.")
    port: conint(ge=1, le=65535) = 502
    unit_id: conint(ge=0, le=255) = 1
    enabled: bool = True


class ModbusConfig(BaseModel):
    site_id: str
    devices: List[ModbusDevice] = Field(default_factory=list)

    def to_modbus_targets_env(self) -> str:
        # MODBUS_TARGETS=ip:port:unitId,ip:port:unitId
        parts = []
        for d in self.devices:
            if d.enabled:
                parts.append(f"{d.host}:{d.port}:{d.unit_id}")
        return ",".join(parts)
