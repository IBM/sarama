# Describe cluster example

This example shows how to invoke Kafka's [DescribeCluster](https://kafka.apache.org/protocol.html#The_Messages_DescribeCluster) API (key 60) directly through Sarama. It connects to the cluster controller, issues the request, and prints the cluster ID, controller ID, endpoint type, authorized operations, and the broker list (including the fenced bit on Kafka 4.0+).

在仓库根目录下运行：

```bash
cd examples/describe_cluster
go run . \
  -brokers="localhost:9092" \
  -version="3.7.0.0" \
  -endpoint-type=brokers \
  -include-cluster-ops
```

Flags:

- `-brokers`: Bootstrap broker list (comma separated). Required.
- `-version`: Kafka protocol version used for request negotiation. Determines the DescribeCluster version.
- `-endpoint-type`: `brokers` or `controllers` (Kafka 3.7+).
- `-include-cluster-ops`: Request the cluster authorized operations bitfield.
- `-include-fenced-brokers`: Whether to include fenced brokers when the cluster supports DescribeCluster v2 (Kafka 4.0+).
- `-sasl-mechanism`: Set to `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512` when the cluster requires SASL authentication (provide `-sasl-user` and `-sasl-password` as well).
- `-sasl-user`, `-sasl-password`, `-sasl-authzid`: Credentials used when SASL is enabled.
- `-verbose`: Enable Sarama's internal logging for quick troubleshooting.
