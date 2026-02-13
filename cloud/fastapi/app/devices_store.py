import json
from pathlib import Path
from typing import Any, Dict, List

# File JSON versionato (o bind-mountato) dove tieni i device per sito
_REG_PATH = Path(__file__).parent / "storage" / "devices_registry.json"

def _load_registry() -> Dict[str, Any]:
    if not _REG_PATH.exists():
        return {}
    return json.loads(_REG_PATH.read_text(encoding="utf-8") or "{}")

def _save_registry(reg: Dict[str, Any]) -> None:
    _REG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _REG_PATH.write_text(json.dumps(reg, indent=2, ensure_ascii=False), encoding="utf-8")

def get_site_devices(site_id: str) -> List[Dict[str, Any]]:
    reg = _load_registry()
    # formato atteso: { "PV_001": [ {...}, {...} ], "PV_002": [...] }
    devices = reg.get(site_id, [])
    if devices is None:
        return []
    if not isinstance(devices, list):
        # fallback: se il file è in formato diverso, non crashare l'API
        return []
    return devices

def set_site_devices(site_id: str, devices: List[Dict[str, Any]]) -> None:
    reg = _load_registry()
    reg[site_id] = devices if isinstance(devices, list) else []
    _save_registry(reg)
