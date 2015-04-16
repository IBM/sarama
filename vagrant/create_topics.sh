#!/bin/sh

set -ex

while ! nc -q 1 localhost 21801 </dev/null; do echo "Waiting"; sleep 1; done
while ! nc -q 1 localhost 29092 </dev/null; do echo "Waiting"; sleep 1; done

cd ${KAFKA_INSTALL_ROOT}/kafka-9092
bin/kafka-topics.sh --create --partitions 1 --replication-factor 3 --topic test.1 --zookeeper localhost:2181
bin/kafka-topics.sh --create --partitions 4 --replication-factor 3 --topic test.4 --zookeeper localhost:2181
bin/kafka-topics.sh --create --partitions 64 --replication-factor 3 --topic test.64  --zookeeper localhost:2181
