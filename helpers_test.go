package sarama

import (
	"io"
	"strconv"
	"sync"
	"testing"
	"time"
)

func safeClose(t testing.TB, c io.Closer) {
	t.Helper()
	err := c.Close()
	if err != nil {
		t.Error(err)
	}
}

func closeProducerWithTimeout(t *testing.T, p AsyncProducer, timeout time.Duration) {
	var wg sync.WaitGroup
	p.AsyncClose()

	closer := make(chan struct{})
	timer := time.AfterFunc(timeout, func() {
		t.Error("timeout")
		close(closer)
	})
	defer timer.Stop()

	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-closer:
				return
			case _, ok := <-p.Successes():
				if !ok {
					return
				}
				t.Error("Unexpected message on Successes()")
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-closer:
				return
			case msg, ok := <-p.Errors():
				if !ok {
					return
				}
				t.Error(msg.Err)
			}
		}
	}()
	wg.Wait()
}

func closeProducer(t *testing.T, p AsyncProducer) {
	closeProducerWithTimeout(t, p, 5*time.Minute)
}

const TestMessage = "ABC THE MESSAGE"

type appendInterceptor struct {
	i int
}

func (b *appendInterceptor) OnSend(msg *ProducerMessage) {
	if b.i < 0 {
		panic("hey, the interceptor has failed")
	}
	v, _ := msg.Value.Encode()
	msg.Value = StringEncoder(string(v) + strconv.Itoa(b.i))
	b.i++
}

func (b *appendInterceptor) OnConsume(msg *ConsumerMessage) {
	if b.i < 0 {
		panic("hey, the interceptor has failed")
	}
	msg.Value = []byte(string(msg.Value) + strconv.Itoa(b.i))
	b.i++
}
