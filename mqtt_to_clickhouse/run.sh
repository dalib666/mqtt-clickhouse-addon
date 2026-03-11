#!/usr/bin/with-contenv bashio
set -e

export MQTT_HOST="$(bashio::config 'mqtt_host')"
export MQTT_PORT="$(bashio::config 'mqtt_port')"
export MQTT_TOPIC="$(bashio::config 'mqtt_topic')"
export MQTT_USERNAME="$(bashio::config 'mqtt_username')"
export MQTT_PASSWORD="$(bashio::config 'mqtt_password')"

export CLICKHOUSE_HOST="$(bashio::config 'clickhouse_host')"
export CLICKHOUSE_PORT="$(bashio::config 'clickhouse_port')"
export CLICKHOUSE_USER="$(bashio::config 'clickhouse_user')"
export CLICKHOUSE_PASSWORD="$(bashio::config 'clickhouse_password')"
export CLICKHOUSE_DATABASE="$(bashio::config 'clickhouse_database')"
export CLICKHOUSE_TABLE="$(bashio::config 'clickhouse_table')"

export BATCH_SIZE="$(bashio::config 'batch_size')"
export FLUSH_INTERVAL_SEC="$(bashio::config 'flush_interval_sec')"

exec python3 /app/MQTTtoClickHouse.py
