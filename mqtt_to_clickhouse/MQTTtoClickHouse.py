import os
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import clickhouse_connect
import paho.mqtt.client as mqtt


# =========================
# CONFIG
# =========================
MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "ha_statestream/#")
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "ha_sensor_numeric")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
FLUSH_INTERVAL_SEC = int(os.getenv("FLUSH_INTERVAL_SEC", "5"))


# =========================
# CLICKHOUSE CLIENT
# =========================
ch_client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE,
)


def ensure_clickhouse_table() -> None:
    with clickhouse_lock:
        ch_client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE} (
                entity_id String,
                last_changed DateTime64(3, 'UTC'),
                value Float64
            )
            ENGINE = MergeTree
            ORDER BY (entity_id, last_changed)
            """
        )


# =========================
# BUFFER
# =========================
buffer: List[Tuple[str, datetime, float]] = []
entity_timestamps: Dict[str, Dict[str, datetime]] = {}
entity_states: Dict[str, float] = {}
last_published_last_changed: Dict[str, datetime] = {}
last_flush_time = time.time()
state_lock = threading.Lock()
clickhouse_lock = threading.Lock()


# =========================
# HELPERS
# =========================
def parse_statestream_topic(topic: str) -> Optional[Tuple[str, str]]:
    """
    Converts MQTT topic:
        ha_statestream/sensor/grid_power/state
    to:
        ("sensor.grid_power", "state")
    """
    parts = topic.split("/")
    if len(parts) < 4:
        return None

    _base_topic = parts[0]
    domain = parts[1]
    object_id = "/".join(parts[2:-1])
    attribute = parts[-1]

    if not domain or not object_id or not attribute:
        return None

    return (f"{domain}.{object_id}", attribute)


def parse_iso_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def parse_numeric_state(state_raw) -> Optional[float]:
    if state_raw is None:
        return None

    if isinstance(state_raw, (int, float)):
        return float(state_raw)

    if not isinstance(state_raw, str):
        return None

    state_raw = state_raw.strip()

    if state_raw.lower() in {"unknown", "unavailable", "none", "null", ""}:
        return None

    try:
        return float(state_raw)
    except ValueError:
        return None


def update_entity_timestamps(entity_id: str, attribute: str, payload: str) -> None:
    if attribute != "last_changed":
        return

    timestamp = parse_iso_dt(payload.strip().strip('"'))
    if timestamp is None:
        return

    entity_timestamps.setdefault(entity_id, {})[attribute] = timestamp


def build_row_if_changed(entity_id: str) -> Optional[Tuple[str, datetime, float]]:
    value = entity_states.get(entity_id)
    if value is None:
        return None

    timestamps = entity_timestamps.get(entity_id, {})
    last_changed = timestamps.get("last_changed")
    if last_changed is None:
        return None

    previous_last_changed = last_published_last_changed.get(entity_id)
    if previous_last_changed == last_changed:
        return None

    last_published_last_changed[entity_id] = last_changed
    return entity_id, last_changed, value


def parse_mqtt_statestream_message(topic: str, payload: str) -> Optional[Tuple[str, datetime, float]]:
    parsed_topic = parse_statestream_topic(topic)
    if not parsed_topic:
        return None

    entity_id, attribute = parsed_topic
    payload = payload.strip()

    if attribute == "last_changed":
        update_entity_timestamps(entity_id, attribute, payload)
        return build_row_if_changed(entity_id)

    if attribute != "state":
        return None

    value = parse_numeric_state(payload.strip('"'))
    if value is None:
        return None

    entity_states[entity_id] = value
    return build_row_if_changed(entity_id)


def flush_buffer(force: bool = False) -> None:
    global buffer, last_flush_time

    now = time.time()
    with state_lock:
        should_flush = force or len(buffer) >= BATCH_SIZE or (
            buffer and (now - last_flush_time >= FLUSH_INTERVAL_SEC)
        )

        if not should_flush:
            return

        rows = buffer
        buffer = []
        last_flush_time = now

    try:
        with clickhouse_lock:
            ch_client.insert(
                CLICKHOUSE_TABLE,
                rows,
                column_names=["entity_id", "last_changed", "value"],
            )
        print(f"Inserted {len(rows)} rows into ClickHouse")
    except Exception as e:
        print(f"ClickHouse insert failed: {e}")
        with state_lock:
            buffer = rows + buffer


# =========================
# MQTT CALLBACKS
# =========================
def on_connect(client, userdata, flags, rc, properties=None):
    is_failure = getattr(rc, "is_failure", rc != 0)
    if is_failure:
        print(f"MQTT connection rejected: rc={rc}")
        return

    print(f"Connected to MQTT with rc={rc}")
    client.subscribe(MQTT_TOPIC)
    print(f"Subscribed to: {MQTT_TOPIC}")


def on_message(client, userdata, msg):
    global buffer

    try:
        payload = msg.payload.decode("utf-8", errors="ignore")
        with state_lock:
            row = parse_mqtt_statestream_message(msg.topic, payload)

            if row is None:
                return

            buffer.append(row)
        flush_buffer(force=False)

    except Exception as e:
        print(f"Error processing message from topic {msg.topic}: {e}")


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    print(f"Disconnected from MQTT: reason_code={reason_code}, flags={disconnect_flags}")


# =========================
# MAIN
# =========================
def main():
    ensure_clickhouse_table()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)

    try:
        client.loop_start()

        while True:
            time.sleep(1)
            flush_buffer(force=False)

    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        flush_buffer(force=True)
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
