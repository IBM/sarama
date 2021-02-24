module github.com/Shopify/sarama/examples/interceptors

go 1.16

replace github.com/Shopify/sarama => ../../

require (
	github.com/Shopify/sarama v1.27.0
	go.opentelemetry.io/otel v0.10.0
	go.opentelemetry.io/otel/exporters/stdout v0.10.0
	google.golang.org/genproto v0.0.0-20200331122359-1ee6d9798940 // indirect
)
