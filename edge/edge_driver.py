import json, os, sqlite3, time
from dataclasses import dataclass
from typing import Any, Dict, Tuple
from pymodbus.client import ModbusTcpClient
import paho.mqtt.client as mqtt

DB_PATH=os.getenv("BUFFER_DB","/app/data/buffer.sqlite3")

@dataclass
class Cfg:
    site_id:str; modbus_host:str; modbus_port:int
    mqtt_host:str; mqtt_port:int; mqtt_user:str; mqtt_pass:str
    mqtt_ca:str; mqtt_insecure:bool; topic_base:str; publish_interval_s:int; client_id:str

def env_bool(name:str, default:bool=False)->bool:
    v=os.getenv(name); 
    return default if v is None else v.strip().lower() in ("1","true","yes","y","on")

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con=sqlite3.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS outbox(id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, payload TEXT)")
    con.commit(); con.close()

def enqueue(payload:Dict[str,Any]):
    con=sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO outbox(ts,payload) VALUES(?,?)",(int(time.time()), json.dumps(payload)))
    con.commit(); con.close()

def drain(mq:mqtt.Client, topic:str, max_n:int=200)->int:
    con=sqlite3.connect(DB_PATH); sent=0
    rows=con.execute("SELECT id,payload FROM outbox ORDER BY id LIMIT ?",(max_n,)).fetchall()
    for rid, txt in rows:
        info=mq.publish(topic, txt, qos=1)
        info.wait_for_publish(timeout=5)
        if info.rc!=mqtt.MQTT_ERR_SUCCESS: break
        con.execute("DELETE FROM outbox WHERE id=?",(rid,)); sent+=1
    con.commit(); con.close(); return sent

def read_modbus(client:ModbusTcpClient)->Tuple[Dict[str,Any],Dict[str,Any]]:
    rr=client.read_holding_registers(address=0, count=2, slave=1)
    if rr.isError(): raise TimeoutError("Modbus read error")
    return {"p_ac_w": float(rr.registers[0]), "poa_wm2": float(rr.registers[1])}, {}

def main():
    cfg=Cfg(
        site_id=os.getenv("SITE_ID","PV_001"),
        modbus_host=os.getenv("MODBUS_HOST","192.168.2.108"),
        modbus_port=int(os.getenv("MODBUS_PORT","502")),
        mqtt_host=os.getenv("MQTT_HOST","host.docker.internal"),
        mqtt_port=int(os.getenv("MQTT_PORT","8883")),
        mqtt_user=os.getenv("MQTT_USERNAME",""),
        mqtt_pass=os.getenv("MQTT_PASSWORD",""),
        mqtt_ca=os.getenv("MQTT_TLS_CA","/app/certs/ca.crt"),
        mqtt_insecure=env_bool("MQTT_TLS_INSECURE", False),
        topic_base=os.getenv("MQTT_TOPIC_BASE","pv"),
        publish_interval_s=int(os.getenv("PUBLISH_INTERVAL_S","5")),
        client_id=f"edge-{os.getenv('SITE_ID','PV_001')}",
    )
    init_db()
    topic=f"{cfg.topic_base}/{cfg.site_id}/telemetry"
    mq=mqtt.Client(client_id=cfg.client_id, protocol=mqtt.MQTTv5)
    mq.username_pw_set(cfg.mqtt_user, cfg.mqtt_pass)
    mq.tls_set(ca_certs=cfg.mqtt_ca)
    mq.tls_insecure_set(cfg.mqtt_insecure)

    while True:
        try:
            mq.connect(cfg.mqtt_host, cfg.mqtt_port, keepalive=30)
            break
        except Exception:
            time.sleep(2)
    mq.loop_start()

    mb=ModbusTcpClient(host=cfg.modbus_host, port=cfg.modbus_port, timeout=2)

    while True:
        try:
            if not mb.connect(): raise TimeoutError("Modbus connect failed")
            fields, tags = read_modbus(mb)
            payload={"site_id":cfg.site_id,"measurement":"pv_telemetry","ts":time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),"tags":tags,"fields":fields}
            enqueue(payload)
            if mq.is_connected():
                drain(mq, topic, max_n=500)
        except Exception:
            pass
        time.sleep(cfg.publish_interval_s)

if __name__=="__main__":
    main()
