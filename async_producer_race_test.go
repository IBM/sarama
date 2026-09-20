package sarama

import "testing"

// retryHandler must not read a message after sending it to p.input;
// the receiving goroutine can mutate it
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

	const numMessages = 10
	received := make(chan struct{})
	// Stands in for dispatcher(), which applies Producer.Interceptors to every
	// message it receives from p.input, retries included. A tracing interceptor
	// appends to msg.Headers.
	go func() {
		defer close(received)
		for range numMessages {
			msg := <-p.input
			msg.Headers = append(msg.Headers, RecordHeader{Key: []byte("traceparent")})
		}
	}()

	for range numMessages {
		p.retries <- &ProducerMessage{Topic: "t", Value: StringEncoder("x")}
	}
	<-received
	close(p.retries)
	<-done
	close(p.input)
}
