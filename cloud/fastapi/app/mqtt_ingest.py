def write_edge_status(influx, site_id: str, payload: dict):
    # writes edge connectivity status to influx measurement 'edge_status'
    # payload is the mqtt json received from edge: {site_id, mqtt_connected, edge_id, hostname, ...}
    from influxdb_client import Point, WritePrecision
    p = Point("edge_status").tag("site_id", site_id)

    # write a few boolean/string fields (strings as fields are ok)
    if "mqtt_connected" in payload:
        p = p.field("mqtt_connected", bool(payload.get("mqtt_connected")))
    if "edge_id" in payload:
        p = p.field("edge_id", str(payload.get("edge_id")))
    if "hostname" in payload:
        p = p.field("hostname", str(payload.get("hostname")))

    # Use ts if provided (seconds), else now
    ts = payload.get("ts")
    if isinstance(ts, int):
        p = p.time(ts * 1_000_000_000, WritePrecision.NS)
    influx.write_api().write(bucket=os.getenv("INFLUX_BUCKET",""), org=os.getenv("INFLUX_ORG",""), record=p)
import json, os, threading, time
from typing import Optional
import paho.mqtt.client as mqtt
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from app.influx import get_influx_client

_thread: Optional[threading.Thread] = None

def _env_bool(name:str, default:bool=False)->bool:
    v=os.getenv(name)
    if v is None: return default
    return v.strip().lower() in ("1","true","yes","y","on")

def _on_connect(client, userdata, flags, reason_code, properties=None):
    topic=os.getenv("MQTT_SUB_TOPIC","pv/+/telemetry")
    client.subscribe(topic, qos=1)

def _on_message(client, userdata, msg):
    try:
        payload=json.loads(msg.payload.decode("utf-8"))
    except Exception:
        return
    site_id=payload.get("site_id")
    meas=payload.get("measurement","pv_telemetry")
    fields=payload.get("fields",{})
    tags=payload.get("tags",{})
    if not site_id or not isinstance(fields, dict):
        return
    org=os.getenv("INFLUX_ORG","")
    bucket=os.getenv("INFLUX_BUCKET","")
    p=Point(meas).tag("site_id", site_id)
    for k,v in (tags or {}).items():
        if v is not None: p=p.tag(str(k), str(v))
    for k,v in fields.items():
        if v is None: continue
        p=p.field(str(k), v)
    userdata["write_api"].write(bucket=bucket, org=org, record=p, write_precision=WritePrecision.S)

def _run():
    influx=get_influx_client()
    write_api=influx.write_api(write_options=SYNCHRONOUS)
    host=os.getenv("MQTT_HOST","mosquitto")
    port=int(os.getenv("MQTT_PORT","8883"))
    user=os.getenv("MQTT_USERNAME","cr_ingest")
    pw=os.getenv("MQTT_PASSWORD","")
    ca=os.getenv("MQTT_TLS_CA","/mosquitto/certs/ca.crt")
    insecure=_env_bool("MQTT_TLS_INSECURE", False)

    client=mqtt.Client(protocol=mqtt.MQTTv5)
    client.username_pw_set(user, pw)
    client.tls_set(ca_certs=ca)
    client.tls_insecure_set(insecure)
    client.on_connect=_on_connect
    client.on_message=_on_message
    client.user_data_set({"write_api": write_api})

    while True:
        try:
            client.connect(host, port, keepalive=30)
            client.loop_forever(retry_first_connection=True)
        except Exception:
            time.sleep(2)

def start_ingest():
    global _thread
    if _thread and _thread.is_alive(): return
    _thread=threading.Thread(target=_run, daemon=True)
    _thread.start()

