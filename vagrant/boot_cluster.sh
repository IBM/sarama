#!/bin/sh

set -ex

vagrant/run_toxiproxy.sh &

sleep 5

for i in 1 2 3 4 5; do
    KAFKA_PORT=`expr $i + 9090`
    cd ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT} && bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
done

sleep 5

for i in 1 2 3 4 5; do
    KAFKA_PORT=`expr $i + 9090`
    cd ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT} && bin/kafka-server-start.sh -daemon config/server.properties
done
