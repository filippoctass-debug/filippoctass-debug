from typing import Any, Dict

def compute_device_status(device: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stub: restituisce lo status base. Se in futuro vuoi logica (online/offline, last_seen, ecc.)
    la metti qui senza toccare le API.
    """
    return {
        "online": device.get("online", True),
        "last_seen": device.get("last_seen"),
        "health": device.get("health", "unknown"),
    }
