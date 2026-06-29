# Event Streams SASL/OAUTHBEARER (IAM apikey) example

This example connects a Sarama client to IBM Cloud Event Streams using
`SASL_SSL` with the `OAUTHBEARER` mechanism, and lists the topics in the
cluster.

It is the Go equivalent of the Kafka Java client configuration described in the IBM
documentation,
[Using SASL/OAUTHBEARER](https://cloud.ibm.com/docs/EventStreams?topic=EventStreams-kafka_using#using_sasl_oauthbearer):

## How it works

Sarama drives the `OAUTHBEARER` exchange through a
[`sarama.AccessTokenProvider`](https://pkg.go.dev/github.com/IBM/sarama#AccessTokenProvider).
[`iam_token_provider.go`](iam_token_provider.go) implements that interface and
performs the IBM IAM **apikey → bearer token** exchange:

- `POST https://iam.cloud.ibm.com/identity/token`
- `Content-Type: application/x-www-form-urlencoded`
- body: `grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=<APIKEY>`
- the `access_token` field of the JSON response is used as the SASL token.

The token is cached and refreshed shortly before its `expires_in` deadline, so
repeated broker connections reuse the same token until it nears expiry.

## Running

```sh
go run . \
  -brokers "broker-0.example.eventstreams.cloud.ibm.com:9093,broker-1.example.eventstreams.cloud.ibm.com:9093" \
  -apikey  "<your-ibm-cloud-api-key>"
```

Both values can also be supplied via environment variables:

```sh
export KAFKA_BROKERS="broker-0...:9093,broker-1...:9093"
export APIKEY="<your-ibm-cloud-api-key>"
go run .
```

### Flags

| Flag              | Env             | Default                                      | Description                                  |
| ----------------- | --------------- | -------------------------------------------- | -------------------------------------------- |
| `-brokers`        | `KAFKA_BROKERS` | —                                            | Comma-separated bootstrap brokers (host:port)|
| `-apikey`         | `APIKEY`        | —                                            | IBM Cloud API key                            |
| `-token-endpoint` | —               | `https://iam.cloud.ibm.com/identity/token`   | IAM token endpoint URL                       |
| `-version`        | —               | Sarama default                               | Kafka cluster version                        |
