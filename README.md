# Control Room (Cloud + Edge) – PV Fleet Monitoring

Dockerized, secure-by-default architecture for monitoring distributed photovoltaic sites.

## Components
- **Cloud** (your PC now, VPS later): Mosquitto (TLS), InfluxDB 2.x, FastAPI Control Room (dashboard + APIs + MQTT ingest), Watchtower auto-updates.
- **Edge** (Siemens IOT2050): Python driver reads Modbus TCP, buffers locally (SQLite), publishes telemetry to Cloud MQTT over TLS, Watchtower auto-updates.

Dashboard: http://127.0.0.1:8000
