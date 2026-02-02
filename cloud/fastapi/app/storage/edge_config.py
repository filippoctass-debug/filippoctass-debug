import json, os
from typing import Any, Dict

DATA_DIR = os.getenv("CONTROL_ROOM_DATA", "/data")
CFG_PATH = os.path.join(DATA_DIR, "edge_config.json")

def _load_all() -> Dict[str, Any]:
    try:
        with open(CFG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_all(data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(CFG_PATH), exist_ok=True)
    tmp = CFG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, CFG_PATH)

def get_site_config(site_id: str) -> Dict[str, Any]:
    data = _load_all()
    return data.get(site_id, {"site_id": site_id, "devices": []})

def set_site_config(site_id: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    data = _load_all()
    data[site_id] = cfg
    _save_all(data)
    return cfg

def list_sites() -> Dict[str, Any]:
    return _load_all()
