# Interceptors Example

It creates a *Producer* interceptor to produce some [OpenTelemetry](https://github.com/open-telemetry/opentelemetry-go/) spans and also modifies
the intercepted message to include some headers.

``` go
conf.Producer.Interceptors = []sarama.ProducerInterceptor{xxx}
```

## Run the example
- `go run main.go trace_interceptor.go`.
- or `go build and pass different parameters.
``` sh
go build && ./interceptors --h
Usage of ./interceptors:
  -brokers string
        The Kafka brokers to connect to, as a comma separated list (default "localhost:9092")
  -topic string
        The Kafka topic to use (default "default_topic")
```

App will output OpenTelemetry spans for every intercepted message, i.e:

```
go run main.go trace_interceptor.go
[Sarama] 2020/08/11 11:35:56 Initializing new client
[Sarama] 2020/08/11 11:35:56 ClientID is the default of 'sarama', you should consider setting it to something application-specific.
[Sarama] 2020/08/11 11:35:56 ClientID is the default of 'sarama', you should consider setting it to something application-specific.
[Sarama] 2020/08/11 11:35:56 client/metadata fetching metadata for all topics from broker localhost:9092
[Sarama] 2020/08/11 11:35:56 Connected to broker at localhost:9092 (unregistered)
[Sarama] 2020/08/11 11:35:56 client/brokers registered new broker #1 at localhost:9092
[Sarama] 2020/08/11 11:35:56 Successfully initialized new client
INFO[0000] Starting to produce 2 messages every 5s
INFO[0005] producing 2 messages at 2020-08-11T11:36:01-07:00  topic=default_topic
[
  {
    "SpanContext": {
      "TraceID": "2c4210c1d6c2ebe758eb41cbc95a0478",
      "SpanID": "046bfc6d6db17ed7",
      "TraceFlags": 1
    },
    "ParentSpanID": "0000000000000000",
    "SpanKind": 1,
    "Name": "default_topic",
    "StartTime": "2020-08-11T11:36:01.57487-07:00",
    "EndTime": "2020-08-11T11:36:01.574891849-07:00",
    "Attributes": [
      {
        "Key": "messaging.destination_kind",
        "Value": { "Type": "STRING", "Value": "topic" }
      },
      {
        "Key": "span.otel.kind",
        "Value": { "Type": "STRING", "Value": "PRODUCER" }
      },
      {
        "Key": "messaging.system",
        "Value": { "Type": "STRING", "Value": "kafka" }
      },
      {
        "Key": "net.transport",
        "Value": { "Type": "STRING", "Value": "IP.TCP" }
      },
      {
        "Key": "messaging.url",
        "Value": { "Type": "STRING", "Value": "localhost:9092" }
      },
      {
        "Key": "messaging.destination",
        "Value": { "Type": "STRING", "Value": "default_topic" }
      },
      {
        "Key": "messaging.message_id",
        "Value": { "Type": "STRING", "Value": "046bfc6d6db17ed7" }
      }
    ],
    "MessageEvents": null,
    "Links": null,
    "StatusCode": 0,
    "StatusMessage": "",
    "HasRemoteParent": false,
    "DroppedAttributeCount": 0,
    "DroppedMessageEventCount": 0,
    "DroppedLinkCount": 0,
    "ChildSpanCount": 0,
    "Resource": null,
    "InstrumentationLibrary": {
      "Name": "shopify.com/sarama/examples/interceptors",
      "Version": ""
    }
  }
]
[{"SpanContext":{"TraceID":"b3922fbbaab23b16401c353b0ff9ce6b","SpanID":"269f5133c0d0116e","TraceFlags":1},"ParentSpanID":"0000000000000000","SpanKind":1,"Name":"default_topic","StartTime":"2020-08-11T11:36:01.575388-07:00","EndTime":"2020-08-11T11:36:01.575399065-07:00","Attributes":[{"Key":"messaging.destination_kind","Value":{"Type":"STRING","Value":"topic"}},{"Key":"span.otel.kind","Value":{"Type":"STRING","Value":"PRODUCER"}},{"Key":"messaging.system","Value":{"Type":"STRING","Value":"kafka"}},{"Key":"net.transport","Value":{"Type":"STRING","Value":"IP.TCP"}},{"Key":"messaging.url","Value":{"Type":"STRING","Value":"localhost:9092"}},{"Key":"messaging.destination","Value":{"Type":"STRING","Value":"default_topic"}},{"Key":"messaging.message_id","Value":{"Type":"STRING","Value":"269f5133c0d0116e"}}],"MessageEvents":null,"Links":null,"StatusCode":0,"StatusMessage":"","HasRemoteParent":false,"DroppedAttributeCount":0,"DroppedMessageEventCount":0,"DroppedLinkCount":0,"ChildSpanCount":0,"Resource":null,"InstrumentationLibrary":{"Name":"shopify.com/sarama/examples/interceptors","Version":""}}]
[Sarama] 2020/08/11 11:36:01 ClientID is the default of 'sarama', you should consider setting it to something application-specific.
[Sarama] 2020/08/11 11:36:01 producer/broker/1 starting up
[Sarama] 2020/08/11 11:36:01 producer/broker/1 state change to [open] on default_topic/0
[Sarama] 2020/08/11 11:36:01 Connected to broker at localhost:9092 (registered as #1)
^CINFO[0005] terminating the program
INFO[0005] Bye :)
[Sarama] 2020/08/11 11:36:02 Producer shutting down.
```

## Check the produced intercepted messages

Check that messages have some headers added by the interceptor:
``` sh
kafkacat -Cb localhost:9092 -t default_topic -f '\n- %s\nheaders: %h'
```

```
headers: trace_id=235b3424775d8b2f9bf21e458496f447,span_id=50da1552c105e712,message_id=50da1552c105e712
- test message 1/2 from kafka-client-go-test at 2020-08-11T11:36:01-07:00
headers: trace_id=2c4210c1d6c2ebe758eb41cbc95a0478,span_id=046bfc6d6db17ed7,message_id=046bfc6d6db17ed7
- test message 2/2 from kafka-client-go-test at 2020-08-11T11:36:01-07:00
% Reached end of topic default_topic [0] at offset 444
```
