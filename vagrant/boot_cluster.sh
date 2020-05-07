#!/bin/sh

set -ex

# Launch and wait for toxiproxy
${REPOSITORY_ROOT}/vagrant/run_toxiproxy.sh &
while ! nc -q 1 localhost 2185 </dev/null; do echo "Waiting"; sleep 1; done
while ! nc -q 1 localhost 9095 </dev/null; do echo "Waiting"; sleep 1; done

# Launch and wait for Zookeeper
KAFKA_HEAP_OPTS="-Xmx192m -Dzookeeper.admin.enableServer=false"
export KAFKA_HEAP_OPTS
for i in 1 2 3 4 5; do
    KAFKA_PORT=`expr $i + 9090`
    cd ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT} && bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
done
while ! nc -q 1 localhost 21805 </dev/null; do echo "Waiting"; sleep 1; done

# Launch and wait for Kafka
KAFKA_HEAP_OPTS="-Xmx320m"
export KAFKA_HEAP_OPTS
for i in 1 2 3 4 5; do
    KAFKA_PORT=`expr $i + 9090`
    cd ${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT} && bash bin/kafka-server-start.sh -daemon config/server.properties
done
ps auxww | grep -i kafka

N=120
RC=1
set +x
printf "Waiting for kafka5 to become available."
for _ in $(seq 1 "$N"); do
  if nc -z 127.0.0.1 29095 </dev/null; then
      RC=0
      break
  fi
  sleep 1
done
printf "\n"
if [ "$RC" -gt 0 ]; then
  echo 'Error: kafka5 failed to startup' >&2
  find "${KAFKA_INSTALL_ROOT}/kafka-${KAFKA_PORT}" -name "server.log" -print0 | xargs -0 --no-run-if-empty tail -256
  exit ${RC}
fi
