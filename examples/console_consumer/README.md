# Console Consumer example

This example shows you how to use the [Consumer](https://godoc.org/github.com/Shopify/sarama#Consumer), and how to test them using mocks.
The example also shows how to configure some of the Consumer [Config](https://godoc.org/github.com/Shopify/sarama#Config) properties.


```{r, engine='bash', count_lines}
termainal 1:
go run console_consumer.go -client.id="myconsumer" -topic=test-topic \
                           -brokers="localhost:9092,localhost:9192"  \
                           -offset=148 -retry.backoff.ms=5s

terminal 2:
<KAFKA_HOME>/bin/kafka-producer-perf-test.sh --topic test-topic --num-records 5 --record-size 10 --throughput 10 \
                                --producer-props bootstrap.servers=localhost:9092 \
                                 key.serializer=org.apache.common.serialization.StringSerializer \
                                 value.serializer=org.apache.kafka.common.serialization.StringSerializer
```

You should see an output similar to this:
```{r, engine='bash', count_lines}

termainal1:

2016/10/27 17:55:48 Getting messages from Topic (test-topic)
2016/10/27 17:56:01 Topic: test-topic, Partition: 0, Offset: 150, Key: , Value: SSXVNJHPDQ
2016/10/27 17:56:01 Topic: test-topic, Partition: 0, Offset: 151, Key: , Value: SSXVNJHPDQ
2016/10/27 17:56:01 Topic: test-topic, Partition: 0, Offset: 152, Key: , Value: SSXVNJHPDQ
2016/10/27 17:56:01 Topic: test-topic, Partition: 0, Offset: 153, Key: , Value: SSXVNJHPDQ
2016/10/27 17:56:02 Topic: test-topic, Partition: 0, Offset: 154, Key: , Value: SSXVNJHPDQ
```



```{r, engine='bash', count_lines}
./console_consumer --help
Usage of ./console_consumer:
  -brokers string
    	The Kafka brokers to connect to, as a comma separated list. (default ":9092")
  -client.id string
    	A user-provided string sent with every request to the brokers for logging, debugging, and auditing purposes (default "console_consumer")
  -fetch.message.max.bytes int
    	The maximum number of message bytes to fetch from the broker in a single request. 0 = no limits.
  -fetch.min.bytes int
    	The minimum number of message bytes to fetch in a request. (default 1)
  -offset string
    	The offset id to consume from (a non-negative number), or 'earliest' which means from beginning, or'latest' which means from end. (default "latest")
  -partition int
    	The partition to consume from. (default 0)
  -retry.backoff.ms duration
    	How long to wait after a failing to read from a partition before trying again. (default 2s)
  -topic string
    	The topic id to consume on. (default "")
```

