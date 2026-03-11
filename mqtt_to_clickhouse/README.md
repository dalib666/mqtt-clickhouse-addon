# MQTT to ClickHouse Home Assistant add-on

This add-on runs the uploaded `MQTTtoClickHouse.py` script inside Home Assistant.

## Install

1. Copy the `mqtt_to_clickhouse` folder into your Home Assistant local add-ons directory.
2. In Home Assistant go to **Settings → Add-ons → Add-on Store → ⋮ → Repositories** and add the local add-on folder if needed.
3. Reload add-ons.
4. Open the add-on, fill in MQTT and ClickHouse settings, then start it.
5. Enable **Start on boot**.

## Notes

- The add-on writes only numeric `state` values.
- It stores rows when it has both `state` and `last_changed` for an entity.
- The ClickHouse table is created automatically if it does not exist.
