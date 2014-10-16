#/bin/sh

set -ex

for i in 1 2 3 4 5; do
    KAFKA_PORT=`expr $i + 6666`
    nohup bash -c "cd ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT} && bin/zookeeper-server-start.sh config/zookeeper.properties &"
done

for i in 1 2 3 4 5; do
    KAFKA_PORT=`expr $i + 6666`
    nohup bash -c "cd ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT} && bin/kafka-server-start.sh config/server.properties &"
done
