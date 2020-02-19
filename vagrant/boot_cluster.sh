#!/bin/sh

set -ex

# Launch and wait for toxiproxy
${REPOSITORY_ROOT}/vagrant/run_toxiproxy.sh &
while ! nc -q 1 localhost 2181 </dev/null; do echo "Waiting"; sleep 1; done
while ! nc -q 1 localhost 9092 </dev/null; do echo "Waiting"; sleep 1; done

# Launch and wait for Zookeeper
for i in 1 2 3 4 5; do
    KAFKA_PORT=`expr $i + 9090`
    cd ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT} && bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
done
while ! nc -q 1 localhost 21805 </dev/null; do echo "Waiting"; sleep 1; done

# Launch and wait for Kafka
for i in 1 2 3 4 5; do
    KAFKA_PORT=`expr $i + 9090`
    cd ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT} && bash bin/kafka-server-start.sh -daemon config/server.properties
done
ps auxww | grep -i kafka

N=10
RC=1
set +x
printf "Waiting for Kafka to become available."
for _ in $(seq 1 "$N"); do
  if nc -z 127.0.0.1 29095 </dev/null; then
      RC=0
      break
  fi
  printf "."
  sleep 1
done
printf "\n"
if [ "$RC" -gt 0 ]; then
  echo 'Error: Kafka failed to startup' >&2
  find "${KAFKA_INSTALL_ROOT}" -name "server.log" -print0 | xargs tail -256
  exit ${RC}
fi
