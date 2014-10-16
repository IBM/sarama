#!/bin/sh

set -e

sudo apt-get install default-jre

KAFKA_VERSION=0.8.1.1

if [ ! -f /opt/kafka-${KAFKA_VERSION}.tgz ]; then
    wget http://apache.mirror.nexicom.net/kafka/${KAFKA_VERSION}/kafka_2.10-${KAFKA_VERSION}.tgz -O /opt/kafka-${KAFKA_VERSION}.tgz
fi

for i in 1 2 3 4 5; do
    ZK_PORT=`expr $i + 2180`
    KAFKA_PORT=`expr $i + 6666`

    # unpack kafka
    mkdir -p /opt/kafka-${KAFKA_PORT}
    tar xzf /opt/kafka-${KAFKA_VERSION}.tgz -C /opt/kafka-${KAFKA_PORT} --strip-components 1

    # broker configuration
    cp /vagrant/vagrant/server.properties /opt/kafka-${KAFKA_PORT}/config/
    sed -i s/KAFKAID/${KAFKA_PORT}/g /opt/kafka-${KAFKA_PORT}/config/server.properties
    sed -i s/ZK_PORT/${ZK_PORT}/g /opt/kafka-${KAFKA_PORT}/config/server.properties

    # zookeeper configuration
    cp /vagrant/vagrant/zookeeper.properties /opt/kafka-${KAFKA_PORT}/config/
    sed -i s/KAFKAID/${KAFKA_PORT}/g /opt/kafka-${KAFKA_PORT}/config/zookeeper.properties
    sed -i s/ZK_PORT/${ZK_PORT}/g /opt/kafka-${KAFKA_PORT}/config/zookeeper.properties
    mkdir -p /opt/zookeeper-${ZK_PORT}
    echo $i > /opt/zookeeper-${ZK_PORT}/myid

    # set up zk service
    cp /vagrant/vagrant/zookeeper.conf /etc/init/zookeeper-${ZK_PORT}.conf
    sed -i s/KAFKAID/${KAFKA_PORT}/g /etc/init/zookeeper-${ZK_PORT}.conf

    # set up kafka service
    cp /vagrant/vagrant/kafka.conf /etc/init/kafka-${KAFKA_PORT}.conf
    sed -i s/KAFKAID/${KAFKA_PORT}/g /etc/init/kafka-${KAFKA_PORT}.conf
    sed -i s/ZK_PORT/${ZK_PORT}/g /etc/init/kafka-${KAFKA_PORT}.conf
done
