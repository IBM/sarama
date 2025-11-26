# Describe cluster example

This example uses Sarama's `ClusterAdmin.DescribeCluster` helper to exercise the logic added in `admin.go`. When the configured Kafka version is at least 2.8.0 it will use the DescribeCluster API (key 60); otherwise it transparently falls back to the Metadata API, just like the library call.

Run it from the repository root with:

```bash
cd examples/describe_cluster
go run . \
  -brokers="localhost:9092" \
  -version="3.7.0"
```

Flags:

- `-brokers`: Bootstrap broker list (comma separated). Required.
- `-version`: Kafka protocol version negotiated by Sarama. Determines whether DescribeCluster is available.
- `-client-id`: Client identifier sent to the cluster.
- `-sasl-mechanism`: Set to `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512` when the cluster requires SASL authentication (provide `-sasl-user` and `-sasl-password` as well).
- `-sasl-user`, `-sasl-password`, `-sasl-authzid`: Credentials used when SASL is enabled.
- `-verbose`: Enable Sarama's internal logging for quick troubleshooting.
