package main

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/IBM/sarama"
)

type OTelInterceptor struct {
	tracer     trace.Tracer
	fixedAttrs []attribute.KeyValue
}

// NewOTelInterceptor processes span for intercepted messages and add some
// headers with the span data.
func NewOTelInterceptor(brokers []string) *OTelInterceptor {
	oi := OTelInterceptor{}
	oi.tracer = sdktrace.NewTracerProvider().Tracer("github.com/IBM/sarama/examples/interceptors")

	// These are based on the spec, which was reachable as of 2020-05-15
	// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md
	oi.fixedAttrs = []attribute.KeyValue{
		attribute.String("messaging.destination_kind", "topic"),
		attribute.String("span.otel.kind", "PRODUCER"),
		attribute.String("messaging.system", "kafka"),
		attribute.String("net.transport", "IP.TCP"),
		attribute.String("messaging.url", strings.Join(brokers, ",")),
	}
	return &oi
}

const (
	MessageIDHeaderName = "message_id"
	SpanHeaderName      = "span_id"
	TraceHeaderName     = "trace_id"
)

func shouldIgnoreMsg(msg *sarama.ProducerMessage) bool {
	// check message hasn't been here before (retries)
	var traceFound, spanFound, msgIDFound bool
	for _, h := range msg.Headers {
		if string(h.Key) == TraceHeaderName {
			traceFound = true
			continue
		}
		if string(h.Key) == SpanHeaderName {
			spanFound = true
			continue
		}
		if string(h.Key) == MessageIDHeaderName {
			msgIDFound = true
		}
	}
	return traceFound && spanFound && msgIDFound
}

func (oi *OTelInterceptor) OnSend(msg *sarama.ProducerMessage) {
	if shouldIgnoreMsg(msg) {
		return
	}
	_, span := oi.tracer.Start(context.TODO(), msg.Topic)
	defer span.End()
	spanContext := span.SpanContext()
	attWithTopic := append(
		oi.fixedAttrs,
		attribute.String("messaging.destination", msg.Topic),
		attribute.String("messaging.message_id", spanContext.SpanID().String()),
	)
	span.SetAttributes(attWithTopic...)

	// remove existing partial tracing headers if exists
	noTraceHeaders := msg.Headers[:0]
	for _, h := range msg.Headers {
		key := string(h.Key)
		if key != TraceHeaderName && key != SpanHeaderName && key != MessageIDHeaderName {
			noTraceHeaders = append(noTraceHeaders, h)
		}
	}
	traceHeaders := []sarama.RecordHeader{
		{Key: []byte(TraceHeaderName), Value: []byte(spanContext.TraceID().String())},
		{Key: []byte(SpanHeaderName), Value: []byte(spanContext.SpanID().String())},
		{Key: []byte(MessageIDHeaderName), Value: []byte(spanContext.SpanID().String())},
	}
	msg.Headers = append(noTraceHeaders, traceHeaders...)
}
