package sarama

import (
	"sync"
)

var expectationsPool = sync.Pool{
	New: func() interface{} {
		return make(chan *ProducerError, 1)
	},
}

// Reuses expectation channel
type syncProducerOptimized struct {
	*syncProducer
}

// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
func NewSyncProducerOptimized(addrs []string, config *Config) (SyncProducer, error) {
	syncProducerIface, err := NewSyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}
	return &syncProducerOptimized{syncProducerIface.(*syncProducer)}, nil
}

// NewSyncProducerFromClient creates a new SyncProducer using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this producer.
func NewSyncProducerFromOptimized(client Client) (SyncProducer, error) {
	syncProducerIface, err := NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	return &syncProducerOptimized{syncProducerIface.(*syncProducer)}, nil
}

func (sp *syncProducerOptimized) SendMessage(msg *ProducerMessage) (partition int32, offset int64, err error) {
	expectation := expectationsPool.Get().(chan *ProducerError)
	msg.expectation = expectation
	sp.producer.Input() <- msg
	pErr := <-expectation
	msg.expectation = nil
	expectationsPool.Put(expectation)
	if pErr != nil {
		return -1, -1, pErr.Err
	}

	return msg.Partition, msg.Offset, nil
}

func (sp *syncProducerOptimized) SendMessages(msgs []*ProducerMessage) error {
	doneIDx := make(chan int, len(msgs))
	go func() {
		for i, msg := range msgs {
			expectation := expectationsPool.Get().(chan *ProducerError)
			msg.expectation = expectation
			sp.producer.Input() <- msg
			doneIDx <- i
		}
		close(doneIDx)
	}()

	var errors ProducerErrors
	for i := range doneIDx {
		expectation := msgs[i].expectation
		pErr := <-expectation
		msgs[i].expectation = nil
		expectationsPool.Put(expectation)
		if pErr != nil {
			errors = append(errors, pErr)
		}
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}
