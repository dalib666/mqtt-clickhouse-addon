ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache python3 py3-pip \
    && python3 -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir paho-mqtt clickhouse-connect

COPY run.sh /
COPY MQTTtoClickHouse.py /app/MQTTtoClickHouse.py

RUN chmod a+x /run.sh

CMD [ "/run.sh" ]