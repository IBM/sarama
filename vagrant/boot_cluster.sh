#!/bin/sh

set -ex

# Launch and wait for toxiproxy if it is configured
if [ -n "${TOXIPROXY_VERSION}" ]; then
    ${REPOSITORY_ROOT}/vagrant/run_toxiproxy.sh &
    while ! nc -q 1 localhost 2181 </dev/null; do echo "Waiting"; sleep 1; done
    while ! nc -q 1 localhost 9092 </dev/null; do echo "Waiting"; sleep 1; done
    ZOOKEEPER_WAIT_PORT=21805
    KAFKA_WAIT_PORT=29095
else
    ZOOKEEPER_WAIT_PORT=2185
    KAFKA_WAIT_PORT=9095
fi

# Launch and wait for Zookeeper
for i in 1 2 3 4 5; do
    KAFKA_PORT=`expr $i + 9090`
    cd ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT} && bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
done
while ! nc -q 1 localhost ${ZOOKEEPER_WAIT_PORT} </dev/null; do echo "Waiting"; sleep 1; done

# Launch and wait for Kafka
for i in 1 2 3 4 5; do
    KAFKA_PORT=`expr $i + 9090`
    cd ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT} && bin/kafka-server-start.sh -daemon config/server.properties
done
while ! nc -q 1 localhost ${KAFKA_WAIT_PORT} </dev/null; do echo "Waiting"; sleep 1; done
