package main

import (
	"context"
	"strings"

	"github.com/Shopify/sarama"
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/trace"
)

type OTelInterceptor struct {
	tracer     trace.Tracer
	fixedAttrs []kv.KeyValue
}

// NewOTelInterceptor processes span for intercepted messages and add some
// headers with the span data.
func NewOTelInterceptor(brokers []string) *OTelInterceptor {
	oi := OTelInterceptor{}
	oi.tracer = global.TraceProvider().Tracer("shopify.com/sarama/examples/interceptors")

	// These are based on the spec, which was reachable as of 2020-05-15
	// https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/trace/semantic_conventions/messaging.md
	oi.fixedAttrs = []kv.KeyValue{
		kv.String("messaging.destination_kind", "topic"),
		kv.String("span.otel.kind", "PRODUCER"),
		kv.String("messaging.system", "kafka"),
		kv.String("net.transport", "IP.TCP"),
		kv.String("messaging.url", strings.Join(brokers, ",")),
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
	_ = oi.tracer.WithSpan(context.TODO(), msg.Topic,
		func(ctx context.Context) error {
			span := trace.SpanFromContext(ctx)
			spanContext := span.SpanContext()
			attWithTopic := append(
				oi.fixedAttrs,
				kv.String("messaging.destination", msg.Topic),
				kv.String("messaging.message_id", spanContext.SpanID.String()),
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
				{Key: []byte(TraceHeaderName), Value: []byte(spanContext.TraceID.String())},
				{Key: []byte(SpanHeaderName), Value: []byte(spanContext.SpanID.String())},
				{Key: []byte(MessageIDHeaderName), Value: []byte(spanContext.SpanID.String())},
			}
			msg.Headers = append(noTraceHeaders, traceHeaders...)
			return nil
		})
}
