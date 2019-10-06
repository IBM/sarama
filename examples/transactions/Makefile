.PHONY: broker
broker:
	docker run -d -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 \
	   -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=kafka \
	   --name broker-test \
	   -e RUNNING_SAMPLEDATA=0 landoop/fast-data-dev:1.1.1

	@echo "waiting for broker to be up"
	@until $$(curl --output /dev/null --silent --head --fail http://localhost:8083); do \
		printf '.'; \
		sleep 2; \
	done
	@echo

	kafka-topics --zookeeper localhost:2181 --topic test-topic --create --partitions 1 --replication-factor 1

.PHONY: clean
clean:
	docker kill broker-test; docker rm broker-test

.PHONY: read-committed
read-committed:
	kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic --from-beginning --timeout-ms 2000 --isolation-level read_committed

.PHONY: read-uncommitted
read-uncommitted:
	kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic --from-beginning --timeout-ms 2000 --isolation-level read_uncommitted

