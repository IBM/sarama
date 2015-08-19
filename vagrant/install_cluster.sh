#!/bin/sh

set -ex

mkdir -p ${KAFKA_INSTALL_ROOT}
if [ ! -f ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_VERSION}.tgz ]; then
    wget --quiet http://apache.mirror.gtcomm.net/kafka/${KAFKA_VERSION}/kafka_2.10-${KAFKA_VERSION}.tgz -O ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_VERSION}.tgz
fi

if [ -n "${TOXIPROXY_VERSION}" ]; then
    if [ ! -f ${KAFKA_INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION} ]; then
        wget --quiet https://github.com/Shopify/toxiproxy/releases/download/v${TOXIPROXY_VERSION}/toxiproxy-linux-amd64 -O ${KAFKA_INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION}
        chmod +x ${KAFKA_INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION}
    fi
    rm -f ${KAFKA_INSTALL_ROOT}/toxiproxy
    ln -s ${KAFKA_INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION} ${KAFKA_INSTALL_ROOT}/toxiproxy
fi

for i in 1 2 3 4 5; do
    ZK_PORT=`expr $i + 2180`
    KAFKA_PORT=`expr $i + 9090`
    if [ -n "${TOXIPROXY_VERSION}" ]; then
        ZK_PORT_REAL=`expr $i + 21800`
        KAFKA_PORT_REAL=`expr $i + 29090`
        KAFKA_HOST=localhost
    else
        ZK_PORT_REAL=${ZK_PORT}
        KAFKA_PORT_REAL=${KAFKA_PORT}
        KAFKA_HOST=0.0.0.0
    fi

    # unpack kafka
    mkdir -p ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}
    tar xzf ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_VERSION}.tgz -C ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT} --strip-components 1

    # broker configuration
    cp ${REPOSITORY_ROOT}/vagrant/server.properties ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/
    sed -i s/KAFKAID/${KAFKA_PORT}/g ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/server.properties
    sed -i s/KAFKAPORT/${KAFKA_PORT_REAL}/g ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/server.properties
    sed -i s/KAFKAHOST/${KAFKA_HOST}/g ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/server.properties
    sed -i s/KAFKA_HOSTNAME/${KAFKA_HOSTNAME}/g ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/server.properties
    sed -i s/ZK_PORT/${ZK_PORT}/g ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/server.properties

    KAFKA_DATADIR="${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/data"
    mkdir -p ${KAFKA_DATADIR}
    sed -i s#KAFKA_DATADIR#${KAFKA_DATADIR}#g ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/server.properties

    # zookeeper configuration
    cp ${REPOSITORY_ROOT}/vagrant/zookeeper.properties ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/
    sed -i s/KAFKAID/${KAFKA_PORT}/g ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/zookeeper.properties
    sed -i s/ZK_PORT/${ZK_PORT_REAL}/g ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/zookeeper.properties

    ZK_DATADIR="${KAFKA_INSTALL_ROOT}/zookeeper-${ZK_PORT}"
    mkdir -p ${ZK_DATADIR}
    sed -i s#ZK_DATADIR#${ZK_DATADIR}#g ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/zookeeper.properties

    echo $i > ${KAFKA_INSTALL_ROOT}/zookeeper-${ZK_PORT}/myid
done
