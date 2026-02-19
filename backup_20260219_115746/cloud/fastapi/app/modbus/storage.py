from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from .models import ModbusConfig

BASE_DIR = Path(__file__).resolve().parents[1]  # .../app
DATA_DIR = BASE_DIR / "data" / "sites"


def _site_dir(site_id: str) -> Path:
    p = DATA_DIR / site_id
    p.mkdir(parents=True, exist_ok=True)
    return p


def _cfg_path(site_id: str) -> Path:
    return _site_dir(site_id) / "modbus_devices.json"


def load_config(site_id: str) -> ModbusConfig:
    p = _cfg_path(site_id)
    if not p.exists():
        return ModbusConfig(site_id=site_id, devices=[])
    raw = p.read_text(encoding="utf-8")
    obj = json.loads(raw)
    return ModbusConfig.model_validate(obj)


def save_config(cfg: ModbusConfig) -> ModbusConfig:
    p = _cfg_path(cfg.site_id)
    p.write_text(cfg.model_dump_json(indent=2), encoding="utf-8")
    return cfg
