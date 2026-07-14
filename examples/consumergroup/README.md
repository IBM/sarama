# Consumergroup example

This example shows you how to use the Sarama consumer group consumer. The example simply starts consuming the given Kafka topics and logs the consumed messages.

```bash
$ go run main.go -brokers="127.0.0.1:9092" -topics="sarama" -group="example"
```

You can also toggle (pause/resume) the consumption by sending SIGUSR1

## Cooperative rebalancing (KIP-429)

The `cooperative-sticky` assignor retains partitions that do not move during a
rebalance. Select it with `-assignor=cooperative-sticky` on a new group.

Under the cooperative protocol, `Setup` and `Cleanup` run once per member and
`Consume` remains blocked across rebalances. A revoked claim is signaled by
its `Messages()` channel closing.

Migrate an existing group with two rolling restarts. The first introduces the
new strategy while retaining `range` as the common eager strategy:

```bash
$ go run main.go -brokers="127.0.0.1:9092" -topics="sarama" -group="example" \
    -assignor=cooperative-sticky-upgrade
```

After every member uses that configuration, remove `range` with a second
rolling restart:

```bash
$ go run main.go -brokers="127.0.0.1:9092" -topics="sarama" -group="example" \
    -assignor=cooperative-sticky
```
