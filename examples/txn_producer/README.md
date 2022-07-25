# Transactional producer example

This example shows you how to use the Sarama transactional producer. The example simply starts some goroutine that produce endlesslly on associated topic.

```bash
$ go run main.go -brokers="127.0.0.1:9092" -topic="sarama" -producers=10 -record-numbers=10000
```

To ensure transactional-id uniqueness it implement some ***ProducerProvider*** that build a producer appending an integer that grow when producer is created.
You can also see record-rate each 5s printing on stdout.