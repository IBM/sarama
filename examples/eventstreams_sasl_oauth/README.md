# Event Streams SASL/OAUTHBEARER (IAM apikey) example

This example connects a Sarama client to IBM Cloud Event Streams using
`SASL_SSL` with the `OAUTHBEARER` mechanism. It opens a **single** broker
connection and, in a loop, prints the current time and lists the cluster topics
with a metadata request, then sleeps. The connection is deliberately kept open
and busy so that broker-driven SASL re-authentication can be observed.

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

## Re-authentication (`connections.max.reauth.ms`)

When the broker has `connections.max.reauth.ms` set, it reports a session
lifetime in the `SaslAuthenticate` response. Sarama honors
[KIP-368](https://cwiki.apache.org/confluence/display/KAFKA/KIP-368%3A+Allow+SASL+Connections+to+Periodically+Re-Authenticate)
and transparently re-authenticates the **same** connection before that session
expires, calling the token provider again.

### Why a single, busy connection

Re-authentication only fires when a connection is still open **and** a request
is sent on it after the threshold has passed. A `ClusterAdmin` fans its requests
across every broker, so each individual connection sits idle and is closed (by
the broker or a load balancer) long before the threshold — you would only ever
see reconnections, never an in-band re-auth.

This example therefore opens one raw broker connection and pokes it with a
metadata request every `-interval`. Keep the interval short enough that the
connection is never considered idle.

### Reading the log

Sarama's `DebugLogger` is enabled, and [`iam_token_provider.go`](iam_token_provider.go)
logs every time it is asked for a token. There are two distinct patterns:

- **Initial auth / reconnect** — a `[token]` line wrapped in a connection
  handshake:

  ```text
  Completed ApiVersionsRequest V3 to broker-...   ← new connection
  [token] ...
  Session expiration in 3600000 ms and session re-authentication on or after ... ms
  Connected to broker at broker-... (registered as #N)
  ```

- **Re-authentication (KIP-368)** — a `[token]` line with **no** surrounding
  handshake, appearing on the same long-lived connection once the threshold is
  reached:

  ```text
  Session expiration in 3600000 ms and session re-authentication on or after ... ms
  [token] ...
  ```

With a 1-hour session lifetime the first re-authentication appears roughly
51–57 minutes (85–95%) after the connection is established, so the program must
run that long to show it.

Because the IAM token (typically valid for an hour) usually outlives the
re-authentication window, the re-auth often reuses the cached token
(`[token] reusing cached IAM token ...`); a new IAM token is requested only once
the cached one nears expiry.

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

The program loops until interrupted (Ctrl-C).

### Flags

| Flag              | Env             | Default                                      | Description                                  |
| ----------------- | --------------- | -------------------------------------------- | -------------------------------------------- |
| `-brokers`        | `KAFKA_BROKERS` | —                                            | Comma-separated bootstrap brokers (host:port)|
| `-apikey`         | `APIKEY`        | —                                            | IBM Cloud API key                            |
| `-token-endpoint` | —               | `https://iam.cloud.ibm.com/identity/token`   | IAM token endpoint URL                       |
| `-version`        | —               | Sarama default                               | Kafka cluster version                        |
| `-interval`       | —               | `30s`                                        | Delay between metadata requests on connection|
