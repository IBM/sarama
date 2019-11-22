#!/bin/bash

# If the functional tests failed (or some other task) then
# we might want to look into the broker logs
if [ "$TRAVIS_TEST_RESULT" = "1" ]; then
    echo "Dumping Kafka brokers server.log:"
    for i in 1 2 3 4 5; do
        KAFKA_PORT=`expr $i + 9090`
        sed -e "s/^/kafka-${KAFKA_PORT} /" ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/logs/server.log{.*,}
    done
fi

set -ex

for i in 1 2 3 4 5; do
    KAFKA_PORT=`expr $i + 9090`
    cd ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT} && bin/kafka-server-stop.sh
done

for i in 1 2 3 4 5; do
    KAFKA_PORT=`expr $i + 9090`
    cd ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT} && bin/zookeeper-server-stop.sh
done

killall toxiproxy
