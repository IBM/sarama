#/bin/sh

set -ex

for i in 1 2 3 4 5; do
    ZK_PORT=`expr $i + 2180`
    KAFKA_PORT=`expr $i + 6666`

    stop zookeeper-${ZK_PORT} || true

    # set up zk service
    cp ${REPOSITORY_ROOT}/vagrant/zookeeper.conf /etc/init/zookeeper-${ZK_PORT}.conf
    sed -i s/KAFKAID/${KAFKA_PORT}/g /etc/init/zookeeper-${ZK_PORT}.conf

    # set up kafka service
    cp ${REPOSITORY_ROOT}/vagrant/kafka.conf /etc/init/kafka-${KAFKA_PORT}.conf
    sed -i s/KAFKAID/${KAFKA_PORT}/g /etc/init/kafka-${KAFKA_PORT}.conf
    sed -i s/ZK_PORT/${ZK_PORT}/g /etc/init/kafka-${KAFKA_PORT}.conf

    start zookeeper-${ZK_PORT}
done
