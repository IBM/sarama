package sarama

import "testing"

// retryHandler reads a message (via ByteSize) after publishing it to p.input,
// by which point the receiving goroutine owns and mutates it.
func TestRetryHandlerHeadersRace(t *testing.T) {
	conf := NewTestConfig()
	conf.Version = V0_11_0_0 // -> version 2, so ByteSize reads msg.Headers

	p := &asyncProducer{
		conf:    conf,
		retries: make(chan *ProducerMessage),
		input:   make(chan *ProducerMessage),
	}

	done := make(chan struct{})
	go func() { defer close(done); p.retryHandler() }()

	// Stands in for dispatcher(), which applies Producer.Interceptors to every
	// message it receives from p.input, retries included. A tracing interceptor
	// appends to msg.Headers.
	go func() {
		for msg := range p.input {
			msg.Headers = append(msg.Headers, RecordHeader{Key: []byte("traceparent")})
		}
	}()

	for range 10 {
		p.retries <- &ProducerMessage{Topic: "t", Value: StringEncoder("x")}
	}
	close(p.retries)
	<-done
	close(p.input)
}
