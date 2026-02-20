import json
import os
import sqlite3
import time
import socket
import ssl
import logging
import threading
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

def ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)

# ------------------------
# DB buffer (outbox)
# ------------------------
DB_PATH = os.getenv("BUFFER_DB", "/app/data/buffer.sqlite3")

def init_db() -> None:
    ensure_dir(DB_PATH)
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
    rows = con.execute(
        "SELECT id, topic, payload FROM outbox ORDER BY id LIMIT ?",
        (max_n,),
    ).fetchall()
    for rid, topic, txt in rows:
        info = mq.publish(topic, txt, qos=1)
        info.wait_for_publish(timeout=5)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            log.warning("drain: publish failed rc=%s; stop draining", info.rc)
            break
        con.execute("DELETE FROM outbox WHERE id=?", (rid,))
        sent += 1
    con.commit()
    con.close()
    return sent

# ------------------------
# Modbus
# ------------------------
@dataclass
class ModbusTarget:
    host: str
    port: int
    unit_id: int
    name: str

def parse_targets_env() -> List[ModbusTarget]:
    """
    MODBUS_TARGETS format:
      - host:port:unit
      - host:port:unit:name
    comma separated:
      MODBUS_TARGETS=10.0.0.1:502:1:dev1,10.0.0.2:502:2:dev2
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
            out.append(ModbusTarget(host=host, port=port, unit_id=unit, name=name))
        if out:
            return out

    # fallback legacy single target
    host = os.getenv("MODBUS_HOST", "192.168.2.108")
    port = int(os.getenv("MODBUS_PORT", "502"))
    unit = int(os.getenv("MODBUS_UNIT_ID", "1"))
    return [ModbusTarget(host=host, port=port, unit_id=unit, name="dev1")]

def read_modbus_one(t: ModbusTarget) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    mb = ModbusTcpClient(host=t.host, port=t.port, timeout=2)
    try:
        if not mb.connect():
            raise TimeoutError(f"connect failed to {t.host}:{t.port}")
        rr = mb.read_holding_registers(address=0, count=2, slave=t.unit_id)
        if rr.isError():
            raise TimeoutError(f"read error from unit {t.unit_id} at {t.host}:{t.port}")
        fields = {
            "p_ac_w": float(rr.registers[0]),
            "poa_wm2": float(rr.registers[1]),
        }
        tags = {
            "modbus_host": t.host,
            "modbus_port": t.port,
            "unit_id": t.unit_id,
            "device": t.name,  # IMPORTANT: keep "dev1/dev2" to match dashboard
        }
        return fields, tags
    finally:
        try:
            mb.close()
        except Exception:
            pass

# ------------------------
# Runtime targets (from env OR from cmd set_targets)
# ------------------------
TARGETS_FILE = os.getenv("TARGETS_FILE", "/app/data/targets.json")
_targets_lock = threading.Lock()
_targets: List[ModbusTarget] = []

def targets_get() -> List[ModbusTarget]:
    with _targets_lock:
        return list(_targets)

def targets_set(new_targets: List[ModbusTarget], persist: bool = True) -> None:
    global _targets
    with _targets_lock:
        _targets = list(new_targets)

    if persist:
        try:
            ensure_dir(TARGETS_FILE)
            payload = {
                "ts": iso_utc(),
                "targets": [
                    {"name": t.name, "host": t.host, "port": t.port, "unit_id": t.unit_id}
                    for t in new_targets
                ],
            }
            with open(TARGETS_FILE, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            log.info("Persisted targets to %s (n=%d)", TARGETS_FILE, len(new_targets))
        except Exception as e:
            log.warning("Failed to persist targets to %s: %s", TARGETS_FILE, e)

def targets_load_initial() -> None:
    # 1) try persisted file
    try:
        if os.path.exists(TARGETS_FILE):
            with open(TARGETS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            items = data.get("targets") or []
            parsed: List[ModbusTarget] = []
            for i, it in enumerate(items, start=1):
                host = str(it.get("host", "")).strip()
                if not host:
                    continue
                port = int(it.get("port") or 502)
                unit_id = int(it.get("unit_id") or 1)
                name = str(it.get("name") or f"dev{i}").strip() or f"dev{i}"
                parsed.append(ModbusTarget(host=host, port=port, unit_id=unit_id, name=name))
            if parsed:
                targets_set(parsed, persist=False)
                log.info("Loaded targets from %s (n=%d)", TARGETS_FILE, len(parsed))
                return
    except Exception as e:
        log.warning("Failed reading %s, fallback to env: %s", TARGETS_FILE, e)

    # 2) fallback env
    env_targets = parse_targets_env()
    targets_set(env_targets, persist=False)
    log.info("Loaded targets from env (n=%d)", len(env_targets))

# ------------------------
# Config
# ------------------------
@dataclass
class Cfg:
    site_id: str
    edge_id: str   # logical id (published)
    client_id: str # MQTT client id
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
    )

# ------------------------
# MQTT TLS
# ------------------------
def build_tls_context(cfg: Cfg) -> ssl.SSLContext:
    ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=cfg.mqtt_ca)
    if cfg.mqtt_tls_min:
        v = cfg.mqtt_tls_min.lower().replace(" ", "")
        if v in ("tlsv1.2", "tls1.2", "1.2"):
            ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        elif v in ("tlsv1.3", "tls1.3", "1.3"):
            ctx.minimum_version = ssl.TLSVersion.TLSv1_3

    if cfg.mqtt_insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx

# ------------------------
# Main
# ------------------------
def main() -> None:
    cfg = get_cfg()
    init_db()
    targets_load_initial()

    log.info(
        "Starting edge-driver site_id=%s edge_id=%s client_id=%s mqtt=%s:%s",
        cfg.site_id, cfg.edge_id, cfg.client_id, cfg.mqtt_host, cfg.mqtt_port
    )
    log.info("Topics pub=%s status=%s cmd=%s", cfg.topic_pub, cfg.topic_status, cfg.topic_cmd)
    log.info(
        "Targets(initial): %s",
        ", ".join([f"{t.name}@{t.host}:{t.port} unit={t.unit_id}" for t in targets_get()]) or "(none)"
    )
    log.info("TLS ca=%s insecure=%s tls_min=%s", cfg.mqtt_ca, cfg.mqtt_insecure, cfg.mqtt_tls_min or "(default)")

    mq = mqtt.Client(client_id=cfg.client_id, protocol=mqtt.MQTTv5)

    if cfg.mqtt_user:
        mq.username_pw_set(cfg.mqtt_user, cfg.mqtt_pass)

    tls_ctx = build_tls_context(cfg)
    mq.tls_set_context(tls_ctx)
    mq.tls_insecure_set(cfg.mqtt_insecure)
    mq.reconnect_delay_set(min_delay=1, max_delay=30)

    mqtt_state = {"ok": False}

    def publish(topic: str, payload: Dict[str, Any]) -> None:
        txt = safe_json(payload)
        if mqtt_state["ok"]:
            info = mq.publish(topic, txt, qos=1)
            info.wait_for_publish(timeout=5)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                log.warning("publish failed rc=%s => enqueue", info.rc)
                enqueue(topic, payload)
                mqtt_state["ok"] = False
        else:
            enqueue(topic, payload)

    def on_connect(client, userdata, flags, reasonCode, properties=None):
        mqtt_state["ok"] = True
        log.info("MQTT connected rc=%s", reasonCode)

        # IMPORTANT: subscribe to cmd so we can receive set_targets from cloud
        try:
            client.subscribe(cfg.topic_cmd, qos=1)
            log.info("Subscribed to cmd topic: %s", cfg.topic_cmd)
        except Exception as e:
            log.warning("Subscribe failed: %s", e)

        online = {
            "site_id": cfg.site_id,
            "edge_id": cfg.edge_id,
            "status": "online",
            "hostname": hostname(),
            "ts": iso_utc(),
            "mqtt_connected": True,
        }
        publish(cfg.topic_status, online)

    def on_disconnect(client, userdata, reasonCode, properties=None):
        mqtt_state["ok"] = False
        log.warning("MQTT disconnected rc=%s", reasonCode)

    def on_message(client, userdata, msg):
        # cmd handler
        try:
            if msg.topic != cfg.topic_cmd:
                return
            raw = msg.payload.decode("utf-8", errors="ignore").strip()
            if not raw:
                return
            data = json.loads(raw)

            if str(data.get("site_id", "")).strip() != cfg.site_id:
                return

            cmd = str(data.get("cmd", "")).strip().lower()
            if cmd != "set_targets":
                log.info("CMD ignored cmd=%s payload=%s", cmd, raw[:200])
                return

            t_in = data.get("targets") or []
            parsed: List[ModbusTarget] = []
            for i, it in enumerate(t_in, start=1):
                host = str(it.get("host", "")).strip()
                if not host:
                    continue
                port = int(it.get("port") or 502)
                unit_id = int(it.get("unit_id") or 1)
                name = str(it.get("name") or it.get("id") or f"dev{i}").strip() or f"dev{i}"
                parsed.append(ModbusTarget(host=host, port=port, unit_id=unit_id, name=name))

            if not parsed:
                log.warning("CMD set_targets received but parsed targets is empty. raw=%s", raw[:300])
                return

            targets_set(parsed, persist=True)
            log.info(
                "Applied CMD set_targets (n=%d): %s",
                len(parsed),
                ", ".join([f"{t.name}@{t.host}:{t.port} unit={t.unit_id}" for t in parsed]),
            )

            # publish an info/status event (optional, but useful to debug)
            publish(cfg.topic_status, {
                "site_id": cfg.site_id,
                "edge_id": cfg.edge_id,
                "status": "online",
                "ts": iso_utc(),
                "cmd_applied": {"cmd": "set_targets", "n": len(parsed)},
                "targets": [{"name": t.name, "host": t.host, "port": t.port, "unit_id": t.unit_id} for t in parsed],
            })

        except Exception as e:
            log.warning("CMD handler error: %s", e)

    mq.on_connect = on_connect
    mq.on_disconnect = on_disconnect
    mq.on_message = on_message

    # Last will => offline
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

    # Connect loop
    while True:
        try:
            mq.connect(cfg.mqtt_host, cfg.mqtt_port, keepalive=30)
            break
        except Exception as e:
            log.warning("MQTT connect failed: %s (retry in 2s)", e)
            time.sleep(2)

    mq.loop_start()

    last_status = 0.0
    while True:
        now = time.time()

        # periodic online status
        if now - last_status >= cfg.status_interval_s:
            tgs = targets_get()
            status_payload = {
                "site_id": cfg.site_id,
                "edge_id": cfg.edge_id,
                "status": "online",
                "hostname": hostname(),
                "ts": iso_utc(),
                "mqtt_connected": bool(mqtt_state["ok"]),
                "topics": {"pub": cfg.topic_pub, "status": cfg.topic_status, "cmd": cfg.topic_cmd},
                "targets": [{"name": t.name, "host": t.host, "port": t.port, "unit_id": t.unit_id} for t in tgs],
            }
            publish(cfg.topic_status, status_payload)
            last_status = now

        # modbus reads
        any_ok = False
        target_results = []
        tgs = targets_get()  # snapshot
        for t in tgs:
            try:
                fields, tags = read_modbus_one(t)
                any_ok = True
                telemetry_payload = {
                    "site_id": cfg.site_id,
                    "measurement": "pv_telemetry_v2",
                    "ts": iso_utc(),
                    "fields": fields,
                    "tags": tags,
                }
                publish(cfg.topic_pub, telemetry_payload)
                target_results.append({"name": t.name, "ok": True})
            except Exception as e:
                err_payload = {
                    "site_id": cfg.site_id,
                    "ts": iso_utc(),
                    "device": t.name,
                    "host": t.host,
                    "port": t.port,
                    "unit_id": t.unit_id,
                    "error": str(e),
                }
                publish(f"{cfg.topic_base}/{cfg.site_id}/modbus_error", err_payload)
                target_results.append({"name": t.name, "ok": False, "error": str(e)})

        # aggregated modbus status
        publish(f"{cfg.topic_base}/{cfg.site_id}/modbus_status", {
            "site_id": cfg.site_id,
            "ts": iso_utc(),
            "modbus_ok": bool(any_ok),
            "results": target_results,
        })

        # drain outbox
        if mqtt_state["ok"]:
            n = drain(mq, max_n=500)
            if n:
                log.info("drained %d buffered msg(s)", n)

        time.sleep(cfg.publish_interval_s)

if __name__ == "__main__":
    main()
