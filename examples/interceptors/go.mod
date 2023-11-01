module github.com/IBM/sarama/examples/interceptors

go 1.16

replace github.com/IBM/sarama => ../../

require (
	github.com/IBM/sarama v1.41.3
	go.opentelemetry.io/otel v0.10.0
	go.opentelemetry.io/otel/exporters/stdout v0.10.0
)
