import json
import os
import sqlite3
import time
import socket
import ssl
import logging
from dataclasses import dataclass
from typing import Any, Dict, Tuple, List, Optional

from pymodbus.client import ModbusTcpClient
import paho.mqtt.client as mqtt

# ------------------------
# Logging
# ------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("edge-driver")

# ------------------------
# Helpers
# ------------------------
def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    return default if v is None else v.strip().lower() in ("1", "true", "yes", "y", "on")

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v is not None and str(v).strip() != "" else default
    except Exception:
        return default

def hostname() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "unknown"

def now_ts() -> int:
    return int(time.time())

def iso_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def safe_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

def u16_to_str(regs: List[int]) -> str:
    b = bytearray()
    for r in regs:
        b.append((r >> 8) & 0xFF)
        b.append(r & 0xFF)
    return b.decode("latin-1", errors="ignore").rstrip("\x00").strip()

def s16(x: int) -> int:
    x &= 0xFFFF
    return x - 0x10000 if x & 0x8000 else x

def s32_from_u16(hi: int, lo: int) -> int:
    v = ((hi & 0xFFFF) << 16) | (lo & 0xFFFF)
    return v - 0x100000000 if v & 0x80000000 else v

def apply_sf(value: Optional[float], sf: Optional[int]) -> Optional[float]:
    if value is None or sf is None:
        return value
    try:
        return float(value) * (10 ** int(sf))
    except Exception:
        return value

# ------------------------
# DB buffer (outbox)
# ------------------------
DB_PATH = os.getenv("BUFFER_DB", "/app/data/buffer.sqlite3")

def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS outbox(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            topic TEXT NOT NULL,
            payload TEXT NOT NULL
        )
    """)
    con.commit()
    con.close()

def enqueue(topic: str, payload: Dict[str, Any]) -> None:
    txt = safe_json(payload)
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.execute(
        "INSERT INTO outbox(ts, topic, payload) VALUES(?,?,?)",
        (now_ts(), topic, txt),
    )
    con.commit()
    con.close()

def drain(mq: mqtt.Client, max_n: int = 200) -> int:
    sent = 0
    con = sqlite3.connect(DB_PATH, timeout=10)
    try:
        rows = con.execute(
            "SELECT id, topic, payload FROM outbox ORDER BY id LIMIT ?",
            (max_n,),
        ).fetchall()

        for rid, topic, txt in rows:
            try:
                info = mq.publish(topic, txt, qos=1)
                info.wait_for_publish(timeout=5)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    log.warning("drain: publish failed rc=%s; stop draining", info.rc)
                    break
            except Exception as e:
                log.warning("drain: publish error (%s). stop draining.", e)
                break

            con.execute("DELETE FROM outbox WHERE id=?", (rid,))
            sent += 1

        con.commit()
        return sent
    finally:
        con.close()

# ------------------------
# Modbus Targets
# ------------------------
@dataclass
class ModbusTarget:
    host: str
    port: int
    unit_id: int
    name: str
    id: str  # stable device id for cloud/UI

def parse_targets() -> List[ModbusTarget]:
    """
    MODBUS_TARGETS format:
      - host:port:unit
      - host:port:unit:name
      - host:port:unit:name:id

    Example:
      MODBUS_TARGETS=192.168.2.108:5020:1:inv1:inv_1,192.168.2.108:5020:2:inv2:inv_2
    """
    raw = (os.getenv("MODBUS_TARGETS", "") or "").strip()
    out: List[ModbusTarget] = []
    if raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        for i, p in enumerate(parts, start=1):
            toks = p.split(":")
            if len(toks) < 2:
                continue
            host = toks[0].strip()
            port = int((toks[1].strip() or "502"))
            unit = int(toks[2].strip()) if len(toks) >= 3 and toks[2].strip() else 1
            name = toks[3].strip() if len(toks) >= 4 and toks[3].strip() else f"dev{i}"
            dev_id = toks[4].strip() if len(toks) >= 5 and toks[4].strip() else f"inv_{i}"
            out.append(ModbusTarget(host=host, port=port, unit_id=unit, name=name, id=dev_id))
        if out:
            return out

    host = os.getenv("MODBUS_HOST", "192.168.2.108")
    port = int(os.getenv("MODBUS_PORT", "502"))
    unit = int(os.getenv("MODBUS_UNIT_ID", "1"))
    return [ModbusTarget(host=host, port=port, unit_id=unit, name="dev1", id="inv_1")]

# ------------------------
# SunSpec Reader
# ------------------------
SUNS_MARKER = [0x5375, 0x6E53]  # "Su" "nS"
SUNSPEC_ENABLE = env_bool("SUNSPEC_ENABLE", True)

SUNSPEC_SCAN_START = env_int("SUNSPEC_SCAN_START", 0)
SUNSPEC_SCAN_END = env_int("SUNSPEC_SCAN_END", 50000)
SUNSPEC_SCAN_STEP = env_int("SUNSPEC_SCAN_STEP", 2)
SUNSPEC_BASE = os.getenv("SUNSPEC_BASE")
SUNSPEC_MAX_MODELS = env_int("SUNSPEC_MAX_MODELS", 64)
SUNSPEC_MAX_BLOCK_REGS = env_int("SUNSPEC_MAX_BLOCK_REGS", 120)
SUNSPEC_PUBLISH_RAW = env_bool("SUNSPEC_PUBLISH_RAW", True)

def mb_read_holding(mb: ModbusTcpClient, unit: int, address: int, count: int) -> List[int]:
    regs: List[int] = []
    remaining = count
    cur = address
    while remaining > 0:
        n = min(remaining, SUNSPEC_MAX_BLOCK_REGS)
        rr = mb.read_holding_registers(address=cur, count=n, slave=unit)
        if rr.isError():
            raise TimeoutError(f"read_holding_registers error addr={cur} count={n}")
        regs.extend(list(rr.registers))
        remaining -= n
        cur += n
    return regs

def find_sunspec_base(mb: ModbusTcpClient, unit: int) -> int:
    if SUNSPEC_BASE is not None and str(SUNSPEC_BASE).strip() != "":
        try:
            return int(SUNSPEC_BASE)
        except Exception:
            pass

    for addr in range(SUNSPEC_SCAN_START, SUNSPEC_SCAN_END, SUNSPEC_SCAN_STEP):
        rr = mb.read_holding_registers(address=addr, count=2, slave=unit)
        if rr.isError():
            continue
        regs = list(rr.registers)
        if len(regs) == 2 and regs[0] == SUNS_MARKER[0] and regs[1] == SUNS_MARKER[1]:
            return addr

    raise RuntimeError(
        f"SunSpec marker not found. Tried scan {SUNSPEC_SCAN_START}..{SUNSPEC_SCAN_END} step={SUNSPEC_SCAN_STEP}. "
        f"Set SUNSPEC_BASE or adjust scan range."
    )

@dataclass
class SunSpecModel:
    model_id: int
    length: int
    data: List[int]
    start_addr: int  # where model_id is located

def read_sunspec_models(mb: ModbusTcpClient, unit: int, base_addr: int) -> List[SunSpecModel]:
    models: List[SunSpecModel] = []
    ptr = base_addr + 2

    for _ in range(SUNSPEC_MAX_MODELS):
        hdr = mb_read_holding(mb, unit, ptr, 2)
        model_id = hdr[0] & 0xFFFF
        length = hdr[1] & 0xFFFF
        if model_id == 0xFFFF:
            break
        if length == 0 or length > 20000:
            raise RuntimeError(f"Invalid SunSpec model length={length} at addr={ptr} model_id={model_id}")

        data = mb_read_holding(mb, unit, ptr + 2, length)
        models.append(SunSpecModel(model_id=model_id, length=length, data=data, start_addr=ptr))
        ptr = ptr + 2 + length

    return models

# ---- Best-effort parsers (models 1, 101, 103) ----
def parse_model_1_common(m: SunSpecModel) -> Dict[str, Any]:
    d = m.data
    def take(start: int, n: int) -> List[int]:
        return d[start:start+n] if start+n <= len(d) else []

    out: Dict[str, Any] = {
        "manufacturer": u16_to_str(take(0, 16)),
        "model": u16_to_str(take(16, 16)),
        "version": u16_to_str(take(40, 8)),
        "serial": u16_to_str(take(48, 16)),
    }
    return {k: v for k, v in out.items() if v}

def parse_model_101(m: SunSpecModel) -> Dict[str, Any]:
    d = m.data
    if len(d) < 20:
        return {}
    try:
        # Common-ish positions for 101 (best effort, varies by vendor)
        A = s16(d[0]); A_SF = s16(d[2])
        Vph = s16(d[3]); V_SF = s16(d[7]) if len(d) > 7 else None
        W = s16(d[8]) if len(d) > 8 else None; W_SF = s16(d[9]) if len(d) > 9 else None
        Hz = s16(d[10]) if len(d) > 10 else None; Hz_SF = s16(d[11]) if len(d) > 11 else None

        # energy (often 32-bit later; if present, try a common spot)
        energy_wh = None
        if len(d) > 24:
            # vendor-dependent; keep best-effort disabled unless sure
            pass

        out = {
            "p_ac_w": apply_sf(W, W_SF) if W is not None else None,
            "grid_v": apply_sf(Vph, V_SF) if Vph is not None else None,
            "freq_hz": apply_sf(Hz, Hz_SF) if Hz is not None else None,
            "i_ac_a": apply_sf(A, A_SF) if A is not None else None,
        }
        return {k: v for k, v in out.items() if v is not None}
    except Exception:
        return {}

def parse_model_103(m: SunSpecModel) -> Dict[str, Any]:
    d = m.data
    if len(d) < 20:
        return {}
    try:
        A = s16(d[0]); A_SF = s16(d[4])
        VphA = s16(d[8]); V_SF = s16(d[11])
        W = s16(d[12]); W_SF = s16(d[13])
        Hz = s16(d[14]); Hz_SF = s16(d[15])

        out = {
            "p_ac_w": apply_sf(W, W_SF),
            "grid_v": apply_sf(VphA, V_SF),
            "freq_hz": apply_sf(Hz, Hz_SF),
            "i_ac_a": apply_sf(A, A_SF),
        }
        return {k: v for k, v in out.items() if v is not None}
    except Exception:
        return {}

def sunspec_to_fields(models: List[SunSpecModel]) -> Dict[str, Any]:
    fields: Dict[str, Any] = {}
    for m in models:
        if m.model_id == 1:
            fields.update(parse_model_1_common(m))
        elif m.model_id == 101:
            fields.update(parse_model_101(m))
        elif m.model_id == 103:
            fields.update(parse_model_103(m))
    return fields

def sunspec_raw(models: List[SunSpecModel], base: int) -> Dict[str, Any]:
    return {
        "base": base,
        "models": [
            {"model_id": m.model_id, "length": m.length, "start_addr": m.start_addr, "regs": m.data}
            for m in models
        ]
    }

# ------------------------
# Reading one inverter
# ------------------------
def read_modbus_one(t: ModbusTarget) -> Tuple[Dict[str, Any], Dict[str, Any], Optional[Dict[str, Any]]]:
    mb = ModbusTcpClient(host=t.host, port=t.port, timeout=2)
    try:
        if not mb.connect():
            raise TimeoutError(f"connect failed to {t.host}:{t.port}")

        tags = {
            "device": t.name,
            "device_id": t.id,
            "modbus_host": t.host,
            "modbus_port": t.port,
            "unit_id": t.unit_id,
        }

        if not SUNSPEC_ENABLE:
            # legacy fallback (kept for safety)
            rr = mb.read_holding_registers(address=0, count=2, slave=t.unit_id)
            if rr.isError():
                raise TimeoutError(f"legacy read error from unit {t.unit_id} at {t.host}:{t.port}")
            fields = {"p_ac_w": float(rr.registers[0]), "poa_wm2": float(rr.registers[1])}
            tags["source"] = "legacy_2regs"
            return fields, tags, None

        base = find_sunspec_base(mb, t.unit_id)
        models = read_sunspec_models(mb, t.unit_id, base)
        fields = sunspec_to_fields(models)
        tags["source"] = "sunspec"
        tags["sunspec_base"] = base

        blob = sunspec_raw(models, base) if SUNSPEC_PUBLISH_RAW else None
        return fields, tags, blob
    finally:
        try:
            mb.close()
        except Exception:
            pass

# ------------------------
# MQTT / Config
# ------------------------
@dataclass
class Cfg:
    site_id: str
    edge_id: str
    client_id: str

    mqtt_host: str
    mqtt_port: int
    mqtt_user: str
    mqtt_pass: str

    mqtt_ca: str
    mqtt_insecure: bool
    mqtt_tls_min: Optional[str]

    topic_base: str
    topic_pub: str
    topic_status: str
    topic_cmd: str

    publish_interval_s: int
    status_interval_s: int

    modbus_targets: List[ModbusTarget]

def get_cfg() -> Cfg:
    site_id = os.getenv("SITE_ID", "PV_001").strip()
    client_id = (os.getenv("MQTT_CLIENT_ID") or f"edge-{site_id}").strip()
    edge_id = (os.getenv("EDGE_ID") or client_id).strip()

    topic_base = os.getenv("MQTT_TOPIC_BASE", "pv").strip()
    topic_pub = os.getenv("MQTT_PUB_TOPIC", f"{topic_base}/{site_id}/telemetry").strip()
    topic_status = os.getenv("MQTT_STATUS_TOPIC", f"{topic_base}/{site_id}/status").strip()
    topic_cmd = os.getenv("MQTT_CMD_TOPIC", f"{topic_base}/{site_id}/cmd").strip()

    return Cfg(
        site_id=site_id,
        edge_id=edge_id,
        client_id=client_id,
        mqtt_host=os.getenv("MQTT_HOST", "host.docker.internal").strip(),
        mqtt_port=int(os.getenv("MQTT_PORT", "8883")),
        mqtt_user=os.getenv("MQTT_USERNAME", "").strip(),
        mqtt_pass=os.getenv("MQTT_PASSWORD", "").strip(),
        mqtt_ca=os.getenv("MQTT_TLS_CA", "/certs/ca.crt").strip(),
        mqtt_insecure=env_bool("MQTT_TLS_INSECURE", False),
        mqtt_tls_min=(os.getenv("MQTT_TLS_MIN") or "").strip() or None,
        topic_base=topic_base,
        topic_pub=topic_pub,
        topic_status=topic_status,
        topic_cmd=topic_cmd,
        publish_interval_s=int(os.getenv("PUBLISH_INTERVAL_S", "5")),
        status_interval_s=int(os.getenv("STATUS_INTERVAL_S", "5")),
        modbus_targets=parse_targets(),
    )

def build_tls_context(cfg: Cfg) -> Optional[ssl.SSLContext]:
    tls_enable = env_bool("MQTT_TLS_ENABLE", True)
    if not tls_enable:
        return None

    cafile = (cfg.mqtt_ca or "").strip()
    cafile_exists = bool(cafile) and os.path.exists(cafile)

    if not cafile_exists:
        if cfg.mqtt_insecure:
            log.warning("TLS enabled but CA missing (%s). Using INSECURE TLS (no verify).", cafile or "(empty)")
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        else:
            raise FileNotFoundError(
                f"TLS CA file not found: {cafile}. Mount it or set MQTT_TLS_INSECURE=true or MQTT_TLS_ENABLE=false."
            )
    else:
        ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=cafile)

    if cfg.mqtt_tls_min:
        v = cfg.mqtt_tls_min.lower().replace(" ", "")
        if v in ("tlsv1.2", "tls1.2", "1.2"):
            ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        elif v in ("tlsv1.3", "tls1.3", "1.3"):
            ctx.minimum_version = ssl.TLSVersion.TLSv1_3

    return ctx

# ------------------------
# Setpoint write (best-effort, address configurable)
# ------------------------
# Per-device override example:
#   SUNSPEC_SETPOINT_ADDR_inv_1=40210
# Fallback:
#   SUNSPEC_SETPOINT_ADDR=40210
#
# Command payload expected (cloud -> edge):
# {
#   "type":"set_active_power_pct",
#   "site_id":"PV_001",
#   "pct": 65,
#   "targets": ["inv_1","inv_2"]  # optional; if missing => all
# }
def get_setpoint_addr(device_id: str) -> Optional[int]:
    k = f"SUNSPEC_SETPOINT_ADDR_{device_id}"
    v = os.getenv(k)
    if v and v.strip():
        try:
            return int(v.strip())
        except Exception:
            return None
    v2 = os.getenv("SUNSPEC_SETPOINT_ADDR")
    if v2 and v2.strip():
        try:
            return int(v2.strip())
        except Exception:
            return None
    return None

def write_setpoint_pct(t: ModbusTarget, pct: int) -> None:
    addr = get_setpoint_addr(t.id)
    if addr is None:
        raise RuntimeError(f"setpoint addr not configured for {t.id}. Set SUNSPEC_SETPOINT_ADDR_{t.id} or SUNSPEC_SETPOINT_ADDR")
    if pct < 0 or pct > 100:
        raise ValueError("pct must be 0..100")

    mb = ModbusTcpClient(host=t.host, port=t.port, timeout=2)
    try:
        if not mb.connect():
            raise TimeoutError(f"connect failed to {t.host}:{t.port}")
        rr = mb.write_register(address=addr, value=int(pct), slave=t.unit_id)
        if rr.isError():
            raise RuntimeError(f"write_register failed addr={addr} pct={pct}")
    finally:
        try:
            mb.close()
        except Exception:
            pass

# ------------------------
# Main
# ------------------------
def main() -> None:
    cfg = get_cfg()
    init_db()

    log.info(
        "Starting edge-driver site_id=%s edge_id=%s client_id=%s mqtt=%s:%s",
        cfg.site_id, cfg.edge_id, cfg.client_id, cfg.mqtt_host, cfg.mqtt_port
    )
    log.info("Topics pub=%s status=%s cmd=%s", cfg.topic_pub, cfg.topic_status, cfg.topic_cmd)
    log.info(
        "Targets: %s",
        ", ".join([f"{t.name}({t.id})@{t.host}:{t.port} unit={t.unit_id}" for t in cfg.modbus_targets]) or "(none)"
    )
    log.info("TLS ca=%s insecure=%s tls_min=%s", cfg.mqtt_ca, cfg.mqtt_insecure, cfg.mqtt_tls_min or "(default)")
    log.info(
        "SunSpec enable=%s base=%s scan=%s..%s step=%s publish_raw=%s",
        SUNSPEC_ENABLE, SUNSPEC_BASE or "(auto)", SUNSPEC_SCAN_START, SUNSPEC_SCAN_END, SUNSPEC_SCAN_STEP, SUNSPEC_PUBLISH_RAW
    )

    mq = mqtt.Client(client_id=cfg.client_id, protocol=mqtt.MQTTv5)
    if cfg.mqtt_user:
        mq.username_pw_set(cfg.mqtt_user, cfg.mqtt_pass)

    tls_ctx = build_tls_context(cfg)
    if tls_ctx is not None:
        mq.tls_set_context(tls_ctx)
        mq.tls_insecure_set(cfg.mqtt_insecure)
    else:
        log.warning("MQTT TLS disabled (MQTT_TLS_ENABLE=false). Using plain TCP.")

    mq.reconnect_delay_set(min_delay=1, max_delay=30)

    mqtt_state = {"ok": False}

    def publish(topic: str, payload: Dict[str, Any]) -> None:
        txt = safe_json(payload)
        if mqtt_state["ok"] and mq.is_connected():
            try:
                info = mq.publish(topic, txt, qos=1)
                info.wait_for_publish(timeout=5)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    log.warning("publish failed rc=%s => enqueue", info.rc)
                    enqueue(topic, payload)
                    mqtt_state["ok"] = False
            except Exception:
                enqueue(topic, payload)
                mqtt_state["ok"] = False
        else:
            enqueue(topic, payload)

    def on_connect(client, userdata, flags, reasonCode, properties=None):
        mqtt_state["ok"] = True
        log.info("MQTT connected rc=%s", reasonCode)

        online = {
            "site_id": cfg.site_id,
            "edge_id": cfg.edge_id,
            "status": "online",
            "hostname": hostname(),
            "ts": iso_utc(),
            "mqtt_connected": True,
            "devices": [
                {"id": t.id, "name": t.name, "host": t.host, "port": t.port, "unit_id": t.unit_id,
                 "setpoint_addr": get_setpoint_addr(t.id)}
                for t in cfg.modbus_targets
            ]
        }
        publish(cfg.topic_status, online)

        # subscribe commands
        mq.subscribe(cfg.topic_cmd, qos=1)

    def on_disconnect(client, userdata, reasonCode, properties=None):
        mqtt_state["ok"] = False
        log.warning("MQTT disconnected rc=%s", reasonCode)

    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8", errors="ignore") or "{}")
        except Exception:
            return

        if not isinstance(payload, dict):
            return

        if payload.get("type") != "set_active_power_pct":
            return
        if str(payload.get("site_id", "")) != str(cfg.site_id):
            return

        pct = payload.get("pct", None)
        try:
            pct_i = int(pct)
        except Exception:
            pct_i = None

        targets = payload.get("targets", None)
        if targets is None:
            selected_ids = [t.id for t in cfg.modbus_targets]
        else:
            selected_ids = [str(x) for x in (targets or [])]

        results = []
        for t in cfg.modbus_targets:
            if t.id not in selected_ids:
                continue
            try:
                write_setpoint_pct(t, pct_i)
                results.append({"id": t.id, "ok": True})
            except Exception as e:
                results.append({"id": t.id, "ok": False, "error": str(e)})

        publish(f"{cfg.topic_base}/{cfg.site_id}/cmd_ack", {
            "site_id": cfg.site_id,
            "ts": iso_utc(),
            "type": "set_active_power_pct",
            "pct": pct_i,
            "targets": selected_ids,
            "results": results,
        })

    mq.on_connect = on_connect
    mq.on_disconnect = on_disconnect
    mq.on_message = on_message

    will_payload = {
        "site_id": cfg.site_id,
        "edge_id": cfg.edge_id,
        "status": "offline",
        "hostname": hostname(),
        "ts": iso_utc(),
        "will": True,
        "mqtt_connected": False,
    }
    mq.will_set(cfg.topic_status, safe_json(will_payload), qos=1, retain=False)

    while True:
        try:
            mq.connect(cfg.mqtt_host, cfg.mqtt_port, keepalive=60)
            break
        except Exception as e:
            log.warning("MQTT connect failed: %s (retry in 2s)", e)
            time.sleep(2)

    mq.loop_start()

    last_status = 0.0

    while True:
        now = time.time()

        if now - last_status >= cfg.status_interval_s:
            publish(cfg.topic_status, {
                "site_id": cfg.site_id,
                "edge_id": cfg.edge_id,
                "status": "online",
                "hostname": hostname(),
                "ts": iso_utc(),
                "mqtt_connected": bool(mqtt_state["ok"]),
            })
            last_status = now

        any_ok = False
        target_results = []

        # aggregation
        sum_pac = 0.0
        sum_count = 0
        online_count = 0

        for t in cfg.modbus_targets:
            try:
                fields, tags, ss_blob = read_modbus_one(t)
                any_ok = True
                online_count += 1

                pac = fields.get("p_ac_w", None)
                if isinstance(pac, (int, float)):
                    sum_pac += float(pac)
                    sum_count += 1

                telemetry_payload: Dict[str, Any] = {
                    "site_id": cfg.site_id,
                    "measurement": "pv_telemetry_v2",
                    "ts": iso_utc(),
                    "fields": fields,
                    "tags": tags,
                }
                if ss_blob is not None:
                    telemetry_payload["sunspec"] = ss_blob

                publish(cfg.topic_pub, telemetry_payload)

                target_results.append({
                    "id": t.id,
                    "name": t.name,
                    "ok": True,
                    "host": t.host,
                    "port": t.port,
                    "unit_id": t.unit_id,
                })
            except Exception as e:
                target_results.append({
                    "id": t.id,
                    "name": t.name,
                    "ok": False,
                    "error": str(e),
                })
                publish(f"{cfg.topic_base}/{cfg.site_id}/modbus_error", {
                    "site_id": cfg.site_id,
                    "ts": iso_utc(),
                    "device_id": t.id,
                    "device": t.name,
                    "host": t.host,
                    "port": t.port,
                    "unit_id": t.unit_id,
                    "error": str(e),
                })

        # PV aggregate topic (for dashboard PV KPI)
        publish(f"{cfg.topic_base}/{cfg.site_id}/pv_aggregate", {
            "site_id": cfg.site_id,
            "ts": iso_utc(),
            "p_ac_sum_w": sum_pac if sum_count else None,
            "devices_total": len(cfg.modbus_targets),
            "devices_online": online_count,
        })

        publish(f"{cfg.topic_base}/{cfg.site_id}/modbus_status", {
            "site_id": cfg.site_id,
            "ts": iso_utc(),
            "modbus_ok": bool(any_ok),
            "results": target_results,
        })

        if mqtt_state["ok"] and mq.is_connected():
            try:
                n = drain(mq, max_n=500)
                if n:
                    log.info("drained %d buffered msg(s)", n)
            except Exception as e:
                log.warning("drain skipped: %s", e)

        time.sleep(cfg.publish_interval_s)

if __name__ == "__main__":
    main()
