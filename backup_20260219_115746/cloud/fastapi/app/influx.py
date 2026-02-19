import os
from influxdb_client import InfluxDBClient

def get_influx_client()->InfluxDBClient:
    url=os.getenv('INFLUX_URL','http://influxdb:8086')
    token=os.getenv('INFLUX_TOKEN','')
    org=os.getenv('INFLUX_ORG','')
    if not token or not org:
        raise RuntimeError('Missing INFLUX_TOKEN or INFLUX_ORG')
    return InfluxDBClient(url=url, token=token, org=org)
