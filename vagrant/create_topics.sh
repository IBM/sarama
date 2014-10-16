#!/bin/sh

set -ex

sleep 10

cd ${KAFKA_INSTALL_ROOT}/kafka-6667
bin/kafka-topics.sh --create --partitions 1 --replication-factor 3 --topic single_partition --zookeeper localhost:2181
bin/kafka-topics.sh --create --partitions 2 --replication-factor 3 --topic multi_partition  --zookeeper localhost:2181
