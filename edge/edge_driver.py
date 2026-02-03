import json, os, sqlite3, time, socket
from dataclasses import dataclass
from typing import Any, Dict, Tuple, List, Optional
from pymodbus.client import ModbusTcpClient
import paho.mqtt.client as mqtt

DB_PATH=os.getenv("BUFFER_DB","/app/data/buffer.sqlite3")

@dataclass
class ModbusTarget:
    host: str
    port: int
    unit_id: int
    name: str

@dataclass
class Cfg:
    site_id:str
    mqtt_host:str; mqtt_port:int; mqtt_user:str; mqtt_pass:str
    mqtt_ca:str; mqtt_insecure:bool; topic_base:str; publish_interval_s:int; client_id:str
    modbus_targets: List[ModbusTarget]

def env_bool(name:str, default:bool=False)->bool:
    v=os.getenv(name)
    return default if v is None else v.strip().lower() in ("1","true","yes","y","on")

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con=sqlite3.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS outbox(id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, topic TEXT, payload TEXT)")
    con.commit(); con.close()

def enqueue(topic:str, payload:Dict[str,Any]):
    con=sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO outbox(ts,topic,payload) VALUES(?,?,?)",(int(time.time()), topic, json.dumps(payload)))
    con.commit(); con.close()

def drain(mq:mqtt.Client, max_n:int=200)->int:
    con=sqlite3.connect(DB_PATH); sent=0
    rows=con.execute("SELECT id,topic,payload FROM outbox ORDER BY id LIMIT ?",(max_n,)).fetchall()
    for rid, topic, txt in rows:
        info=mq.publish(topic, txt, qos=1)
        info.wait_for_publish(timeout=5)
        if info.rc!=mqtt.MQTT_ERR_SUCCESS: break
        con.execute("DELETE FROM outbox WHERE id=?",(rid,)); sent+=1
    con.commit(); con.close(); return sent

def parse_targets() -> List[ModbusTarget]:
    # MODBUS_TARGETS=ip:port:unit,ip:port:unit
    raw = os.getenv("MODBUS_TARGETS","").strip()
    out: List[ModbusTarget] = []
    if raw:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        for i,p in enumerate(parts, start=1):
            # allow ip:port or ip:port:unit
            toks = p.split(":")
            if len(toks) < 2:
                continue
            host = toks[0].strip()
            port = int(toks[1].strip() or "502")
            unit = int(toks[2].strip()) if len(toks) >= 3 and toks[2].strip() != "" else 1
            out.append(ModbusTarget(host=host, port=port, unit_id=unit, name=f"dev{i}"))
        return out

    # fallback legacy
    host=os.getenv("MODBUS_HOST","192.168.2.108")
    port=int(os.getenv("MODBUS_PORT","502"))
    unit=int(os.getenv("MODBUS_UNIT_ID","1"))
    return [ModbusTarget(host=host, port=port, unit_id=unit, name="dev1")]

def read_modbus_one(t: ModbusTarget) -> Tuple[Dict[str,Any], Dict[str,Any]]:
    mb = ModbusTcpClient(host=t.host, port=t.port, timeout=2)
    if not mb.connect():
        raise TimeoutError(f"connect failed to {t.host}:{t.port}")
    rr = mb.read_holding_registers(address=0, count=2, slave=t.unit_id)
    mb.close()
    if rr.isError():
        raise TimeoutError(f"read error from unit {t.unit_id} at {t.host}:{t.port}")
    return (
        {"p_ac_w": float(rr.registers[0]), "poa_wm2": float(rr.registers[1])},
        {"modbus_host": t.host, "modbus_port": t.port, "unit_id": t.unit_id, "device": t.name},
    )

def hostname():
    try:
        return socket.gethostname()
    except Exception:
        return "unknown"

def main():
    cfg=Cfg(
        site_id=os.getenv("SITE_ID","PV_001"),
        mqtt_host=os.getenv("MQTT_HOST","host.docker.internal"),
        mqtt_port=int(os.getenv("MQTT_PORT","8883")),
        mqtt_user=os.getenv("MQTT_USERNAME",""),
        mqtt_pass=os.getenv("MQTT_PASSWORD",""),
        mqtt_ca=os.getenv("MQTT_TLS_CA","/app/certs/ca.crt"),
        mqtt_insecure=env_bool("MQTT_TLS_INSECURE", False),
        topic_base=os.getenv("MQTT_TOPIC_BASE","pv"),
        publish_interval_s=int(os.getenv("PUBLISH_INTERVAL_S","5")),
        client_id=f"edge-{os.getenv('SITE_ID','PV_001')}",
        modbus_targets=parse_targets(),
    )

    init_db()

    topic_telemetry=f"{cfg.topic_base}/{cfg.site_id}/telemetry"
    topic_status=f"{cfg.topic_base}/{cfg.site_id}/status"

    mq=mqtt.Client(client_id=cfg.client_id, protocol=mqtt.MQTTv5)
    mq.username_pw_set(cfg.mqtt_user, cfg.mqtt_pass)
    mq.tls_set(ca_certs=cfg.mqtt_ca)
    mq.tls_insecure_set(cfg.mqtt_insecure)

    mqtt_connected = False

    def publish(topic:str, payload:Dict[str,Any]):
        nonlocal mqtt_connected
        txt = json.dumps(payload)
        if mqtt_connected:
            info = mq.publish(topic, txt, qos=1)
            info.wait_for_publish(timeout=5)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                enqueue(topic, payload)
                mqtt_connected = False
        else:
            enqueue(topic, payload)

    # connect mqtt loop
    while True:
        try:
            mq.connect(cfg.mqtt_host, cfg.mqtt_port, keepalive=30)
            mqtt_connected = True
            break
        except Exception:
            time.sleep(2)
    mq.loop_start()

    last_status = 0

    while True:
        # 1) try to reconnect mqtt if needed
        if not mqtt_connected:
            try:
                mq.reconnect()
                mqtt_connected = True
            except Exception:
                mqtt_connected = False

        # 2) always publish status every 5s (or publish_interval)
        now = time.time()
        if now - last_status >= 5:
            status_payload = {
                "ts": int(now),
                "site_id": cfg.site_id,
                "edge_id": cfg.client_id,
                "hostname": hostname(),
                "mqtt_connected": bool(mqtt_connected),
                "targets": [
                    {"name": t.name, "host": t.host, "port": t.port, "unit_id": t.unit_id}
                    for t in cfg.modbus_targets
                ],
            }
            publish(topic_status, status_payload)
            last_status = now

        # 3) Modbus reads (best-effort). If fails -> publish modbus_error and continue
        any_ok = False
        target_results = []
        for t in cfg.modbus_targets:
            try:
                fields, tags = read_modbus_one(t)
                any_ok = True
                payload = {"ts": int(now), "site_id": cfg.site_id, "fields": fields, "tags": tags}
                publish(topic_telemetry, payload)
                target_results.append({"name": t.name, "ok": True})
            except Exception as e:
                # publish per-target error
                err_payload = {
                    "ts": int(now),
                    "site_id": cfg.site_id,
                    "device": t.name,
                    "host": t.host,
                    "port": t.port,
                    "unit_id": t.unit_id,
                    "error": str(e),
                }
                publish(f"{cfg.topic_base}/{cfg.site_id}/modbus_error", err_payload)
                target_results.append({"name": t.name, "ok": False, "error": str(e)})

        # 4) publish aggregated modbus state (for UI)
        publish(f"{cfg.topic_base}/{cfg.site_id}/modbus_status", {
            "ts": int(now),
            "site_id": cfg.site_id,
            "modbus_ok": bool(any_ok),
            "results": target_results,
        })

        # 5) drain outbox
        if mqtt_connected:
            drain(mq, max_n=200)

        time.sleep(cfg.publish_interval_s)

if __name__=="__main__":
    main()
