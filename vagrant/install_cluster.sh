#!/bin/sh

set -ex

TOXIPROXY_VERSION=2.1.4

mkdir -p ${KAFKA_INSTALL_ROOT}
if [ ! -f ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_VERSION}.tgz ]; then
    wget --quiet https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz -O ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_VERSION}.tgz
fi
if [ ! -f ${KAFKA_INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION} ]; then
    wget --quiet https://github.com/Shopify/toxiproxy/releases/download/v${TOXIPROXY_VERSION}/toxiproxy-server-linux-amd64 -O ${KAFKA_INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION}
    chmod +x ${KAFKA_INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION}
fi
rm -f ${KAFKA_INSTALL_ROOT}/toxiproxy
ln -s ${KAFKA_INSTALL_ROOT}/toxiproxy-${TOXIPROXY_VERSION} ${KAFKA_INSTALL_ROOT}/toxiproxy

for i in 1 2 3 4 5; do
    ZK_PORT=$((i + 2180))
    ZK_PORT_REAL=$((i + 21800))
    KAFKA_PORT=$((i + 9090))
    KAFKA_PORT_REAL=$((i + 29090))

    # unpack kafka
    mkdir -p ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}
    tar xzf ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_VERSION}.tgz -C ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT} --strip-components 1

    # broker configuration
    mkdir -p "${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/data"

    # Append to default server.properties with a small number of customisations
    printf "\n\n" >> "${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/server.properties"
    cat << EOF >> "${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/server.properties"
############################# Sarama Test Cluster #############################

broker.id=${KAFKA_PORT}
broker.rack=${i}

# Listen on "real" port
listeners=PLAINTEXT://:${KAFKA_PORT_REAL}
# Advertise Toxiproxy port
advertised.listeners=PLAINTEXT://${KAFKA_HOSTNAME}:${KAFKA_PORT}

# Connect to Zookeeper via Toxiproxy port
zookeeper.connect=127.0.0.1:${ZK_PORT}

# Data directory
log.dirs="${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/data"

# Create new topics with a replication factor of 2 so failover can be tested
# more easily.
default.replication.factor=2

# Turn on log.retention.bytes to avoid filling up the VM's disk
log.retention.bytes=268435456
log.segment.bytes=268435456

# Enable topic deletion and disable auto-creation
delete.topic.enable=true
auto.create.topics.enable=false

# Lower the zookeeper timeouts so we don't have to wait forever for a node
# to die when we use toxiproxy to kill its zookeeper connection
zookeeper.session.timeout.ms=3000
zookeeper.connection.timeout.ms=3000

# Disable broker ID length constraint
reserved.broker.max.id=10000

# Permit follower fetching (KIP-392)
replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector

###############################################################################
EOF

    # zookeeper configuration
    cp ${REPOSITORY_ROOT}/vagrant/zookeeper.properties ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/
    sed -i s/KAFKAID/${KAFKA_PORT}/g ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/zookeeper.properties
    sed -i s/ZK_PORT/${ZK_PORT_REAL}/g ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/zookeeper.properties

    ZK_DATADIR="${KAFKA_INSTALL_ROOT}/zookeeper-${ZK_PORT}"
    mkdir -p ${ZK_DATADIR}
    sed -i s#ZK_DATADIR#${ZK_DATADIR}#g ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}/config/zookeeper.properties

    echo $i > ${KAFKA_INSTALL_ROOT}/zookeeper-${ZK_PORT}/myid
done
