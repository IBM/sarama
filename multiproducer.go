package sarama

import (
	"sync"
	"time"
)

// MultiProducerConfig is used to pass multiple configuration options to NewProducer.
type MultiProducerConfig struct {
	Partitioner    Partitioner      // Chooses the partition to send messages to, or randomly if this is nil.
	RequiredAcks   RequiredAcks     // The level of acknowledgement reliability needed from the broker (defaults to no acknowledgement).
	Timeout        int32            // The maximum time in ms the broker will wait the receipt of the number of RequiredAcks.
	Compression    CompressionCodec // The type of compression to use on messages (defaults to no compression).
	MaxBufferBytes uint32
	MaxBufferTime  uint32
}

type brokerProducer struct {
	sync.Mutex
	broker        *Broker
	request       *ProduceRequest
	bufferedBytes uint32
	flushNow      chan bool
	stopper       chan bool
}

// MultiProducer publishes Kafka messages on a given topic. It routes messages to the correct broker, refreshing metadata as appropriate,
// and parses responses for errors. You must call Close() on a MultiProducer to avoid leaks, it may not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
type MultiProducer struct {
	m               sync.RWMutex
	client          *Client
	config          MultiProducerConfig
	brokerProducers map[*Broker]*brokerProducer
	errors          chan error
}

// NewMultiProducer creates a new MultiProducer using the given client. The resulting object will buffer/flush Produce messages to Kafka.
func NewMultiProducer(client *Client, config *MultiProducerConfig) (*MultiProducer, error) {
	if config == nil {
		config = new(MultiProducerConfig)
	}

	if config.RequiredAcks < -1 {
		return nil, ConfigurationError("Invalid RequiredAcks")
	}

	if config.Timeout < 0 {
		return nil, ConfigurationError("Invalid Timeout")
	}

	if config.Partitioner == nil {
		config.Partitioner = NewRandomPartitioner()
	}

	if config.MaxBufferBytes == 0 {
		config.MaxBufferBytes = 1
	}

	p := new(MultiProducer)
	p.client = client
	p.config = *config
	p.errors = make(chan error, 16)
	p.brokerProducers = make(map[*Broker]*brokerProducer)

	return p, nil
}

// Close shuts down the MultiProducer and flushes any messages it may have buffered. You must call this function before
// a MultiProducer object passes out of scope, as it may otherwise leak memory. You must call this before calling Close
// on the underlying client.
func (p *MultiProducer) Close() error {
	p.m.Lock()
	defer p.m.Unlock()

	for _, bp := range p.brokerProducers {
		bp.Close()
	}

	return nil
}

// SendMessage sends a message with the given topic, key, and value. The partition to send to is selected by the
// MultiProducer's Partitioner. To send strings as either key or value, see the StringEncoder type.
// If operating in synchronous mode (MaxBufferTime=MaxBufferBytes=0), the error will be returned. If either value is > 0, nil will
// always be returned and you must listen on the channel returned by Errors() to asynchronously receive error replies.
func (p *MultiProducer) SendMessage(topic string, key, value Encoder) error {
	return p.safeSendMessage(topic, key, value, true)
}

func (p *MultiProducer) choosePartition(topic string, key Encoder) (int32, error) {
	partitions, err := p.client.Partitions(topic)
	if err != nil {
		return -1, err
	}

	numPartitions := int32(len(partitions))

	choice := p.config.Partitioner.Partition(key, numPartitions)

	if choice < 0 || choice >= numPartitions {
		return -1, InvalidPartition
	}

	return partitions[choice], nil
}

func (p *MultiProducer) newBrokerProducer(broker *Broker) *brokerProducer {
	bp := &brokerProducer{
		broker:   broker,
		flushNow: make(chan bool),
		stopper:  make(chan bool),
	}

	maxBufferTime := time.Duration(p.config.MaxBufferTime) * time.Millisecond

	initNow := make(chan bool)
	go func() {
		timer := time.NewTimer(maxBufferTime)
		close(initNow)
		for {
			select {
			case <-bp.flushNow:
				p.flush(bp)
			case <-timer.C:
				p.flush(bp)
			case <-bp.stopper:
				p.m.Lock()
				delete(p.brokerProducers, bp.broker)
				p.m.Unlock()
				p.flush(bp)
				p.client.disconnectBroker(bp.broker)
				close(bp.flushNow)
				return
			}
			timer.Reset(maxBufferTime)
		}
	}()
	<-initNow

	return bp
}

func (p *MultiProducer) brokerProducerFor(broker *Broker) *brokerProducer {
	p.m.RLock()
	bp, ok := p.brokerProducers[broker]
	p.m.RUnlock()
	if !ok {
		p.m.Lock()
		bp, ok = p.brokerProducers[broker]
		if !ok {
			bp = p.newBrokerProducer(broker)
			p.brokerProducers[broker] = bp
		}
		p.m.Unlock()
	}
	return bp
}

func (p *MultiProducer) isSynchronous() bool {
	return p.config.MaxBufferTime == 0 && p.config.MaxBufferBytes < 2
}

// Shouldn't be used if operating in synchronous mode.
func (p *MultiProducer) Errors() <-chan error {
	if p.isSynchronous() {
		panic("you can't use Errors() when operating in synchronous mode")
	} else {
		return p.errors
	}
}

func (bp *brokerProducer) addMessage(topic string, partition int32, message *Message, maxBytes uint32) {
	bp.request.AddMessage(topic, partition, message)
	bp.bufferedBytes += uint32(len(message.Key) + len(message.Value))
	if bp.bufferedBytes > maxBytes {
		select {
		case bp.flushNow <- true:
		default:
		}
	}
}

func (p *MultiProducer) newProduceRequest() *ProduceRequest {
	return &ProduceRequest{RequiredAcks: p.config.RequiredAcks, Timeout: p.config.Timeout}
}

func (p *MultiProducer) addMessageForBroker(broker *Broker, topic string, partition int32, keyBytes, valBytes []byte) error {
	bp := p.brokerProducerFor(broker)

	bp.Lock()
	if bp.request == nil {
		bp.request = p.newProduceRequest()
	}
	msg := &Message{Codec: p.config.Compression, Key: keyBytes, Value: valBytes}
	bp.addMessage(topic, partition, msg, p.config.MaxBufferBytes)
	bp.Unlock()

	if p.isSynchronous() {
		return <-p.errors
	} else {
		return nil
	}
}

func (p *MultiProducer) safeSendMessage(topic string, key, value Encoder, retry bool) error {
	partition, err := p.choosePartition(topic, key)
	if err != nil {
		return err
	}

	var keyBytes []byte
	var valBytes []byte

	if key != nil {
		keyBytes, err = key.Encode()
		if err != nil {
			return err
		}
	}
	valBytes, err = value.Encode()
	if err != nil {
		return err
	}

	broker, err := p.client.Leader(topic, partition)
	if err != nil {
		return err
	}

	return p.addMessageForBroker(broker, topic, partition, keyBytes, valBytes)
}

func (bp *brokerProducer) Close() error {
	close(bp.stopper)
	return nil
}

func (p *MultiProducer) flush(bp *brokerProducer) {
	bp.Lock()
	req := bp.request
	bp.request = nil
	bp.bufferedBytes = 0
	bp.Unlock()
	if req != nil {
		p.flushRequest(bp, true, req)
	}
}

// flushRequest must push one and exactly one message onto p.errors when given only one topic-partition.
func (p *MultiProducer) flushRequest(bp *brokerProducer, retry bool, request *ProduceRequest) {

	response, err := bp.broker.Produce(p.client.id, request)

	switch err {
	case nil:
		break
	case EncodingError:
		p.errors <- err
		return
	default:
		if !retry {
			p.errors <- err
			return
		}

		bp.Close()

		for topic, d := range request.msgSets {
			for partition, msgSet := range d {

				otherBroker, err := p.client.Leader(topic, partition)
				if err != nil {
					p.errors <- err
					return
				}
				otherBp := p.brokerProducerFor(otherBroker)

				retryReq := p.newProduceRequest()
				for _, msgBlock := range msgSet.Messages {
					retryReq.AddMessage(topic, partition, msgBlock.Msg)
				}
				p.flushRequest(otherBp, false, retryReq)

			}
		}
	}

	if response == nil {
		p.errors <- nil
		return
	}

	for topic, d := range response.Blocks {
		for partition, block := range d {
			if block == nil {
				p.errors <- IncompleteResponse
				continue
			}

			switch block.Err {
			case NoError:
				p.errors <- nil

			case UnknownTopicOrPartition, NotLeaderForPartition, LeaderNotAvailable:
				if retry {

					msgSet := request.msgSets[topic][partition]

					otherBroker, err := p.client.Leader(topic, partition)
					if err != nil {
						p.errors <- err
						continue
					}
					otherBp := p.brokerProducerFor(otherBroker)

					retryReq := p.newProduceRequest()
					for _, msgBlock := range msgSet.Messages {
						retryReq.AddMessage(topic, partition, msgBlock.Msg)
					}
					p.flushRequest(otherBp, false, retryReq)

				} else {
					p.errors <- block.Err
				}
			default:
				p.errors <- block.Err
			}
		}
	}

}
