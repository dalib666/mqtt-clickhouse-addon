ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache python3 py3-pip \
    && pip3 install --no-cache-dir paho-mqtt clickhouse-connect

WORKDIR /app
COPY MQTTtoClickHouse.py /app/MQTTtoClickHouse.py
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
