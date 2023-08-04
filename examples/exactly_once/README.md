# Exactly-Once example

This example shows you how to use the Sarama transactional producer to ensure exacly-once paradigm with Kafka transaction. The example simply starts consuming the given Kafka topics and produce the consumed message to another topic including current message offset in procuder transaction.

```bash
$ go run main.go -brokers="127.0.0.1:9092" -topics="sarama" -destination-topic="destination-sarama" -group="example"
```

To ensure transactional-id uniqueness it implement some ***ProducerProvider*** that build a producer using current message topic-partition.
You can also toggle (pause/resume) the consumption by sending SIGUSR1.
