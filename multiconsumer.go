package sarama

import (
	"sync"
)

// MultiConsumer processes Kafka messages from an arbitrarily large set of Kafka
// topic-partitions. You MUST call Close() on a MultiConsumer to avoid leaks; it
// will not be garbage-collected otherwise.
//
// It is STRONGLY recommended to pass MinFetchSize and MaxWaitTime options via
// the ConsumerConfig parameter to avoid excessive network traffic. Sensible
// defaults are 50*1024 and 1000, respsectively.
type MultiConsumer struct {
	m sync.RWMutex

	client *Client

	group  string
	config ConsumerConfig

	brokerRunners   map[*Broker]*brokerRunner
	topicPartitions map[string]map[int32]*topicPartition

	waiting       chan *topicPartition
	stopper, done chan bool
	events        chan *MultiConsumerEvent
}

// MultiConsumerEvent type is a sort of Either monad; it contains a Key+Value or
// an error. If Key+Value+Offset is given, it will always contain a
// Topic+Partition. In the error case, it will contain Topic+Partition if
// relevant; otherwise they will be ("",-1).
type MultiConsumerEvent struct {
	Topic      string
	Partition  int32
	Key, Value []byte
	Offset     int64
	Err        error
}

type brokerRunner struct {
	m      sync.Mutex
	intake chan *topicPartition
	Broker *Broker
}

type topicPartition struct {
	Topic     string
	Partition int32
	Offset    int64
	FetchSize int32
}

// NewMultiConsumer creates a new multiconsumer attached to a given client. It
// will read messages as a member of the given consumer group.
func NewMultiConsumer(client *Client, group string, config *ConsumerConfig) (*MultiConsumer, error) {
	if config == nil {
		config = new(ConsumerConfig)
	}

	if config.DefaultFetchSize < 0 {
		return nil, ConfigurationError("Invalid DefaultFetchSize")
	} else if config.DefaultFetchSize == 0 {
		config.DefaultFetchSize = 1024
	}

	if config.MinFetchSize < 0 {
		return nil, ConfigurationError("Invalid MinFetchSize")
	} else if config.MinFetchSize == 0 {
		config.MinFetchSize = 1
	}

	if config.MaxMessageSize < 0 {
		return nil, ConfigurationError("Invalid MaxMessageSize")
	}

	if config.MaxWaitTime < 0 {
		return nil, ConfigurationError("Invalid MaxWaitTime")
	}

	if config.EventBufferSize < 0 {
		return nil, ConfigurationError("Invalid EventBufferSize")
	}

	m := &MultiConsumer{
		client:          client,
		group:           group,
		config:          *config,
		brokerRunners:   make(map[*Broker]*brokerRunner),
		waiting:         make(chan *topicPartition, 16),
		topicPartitions: make(map[string]map[int32]*topicPartition),
		stopper:         make(chan bool),
		done:            make(chan bool),
		events:          make(chan *MultiConsumerEvent, config.EventBufferSize),
	}

	go m.allocator()

	return m, nil
}

// Close stops the multiconsumer from fetching messages. It is required to call
// this function before a multiconsumer object passes out of scope, as it will
// otherwise leak memory. You must call this before calling Close on the
// underlying client.
func (m *MultiConsumer) Close() error {
	close(m.stopper)
	<-m.done
	return nil
}

func (m *MultiConsumer) sendError(topic string, partition int32, err error) bool {
	if err == nil {
		return true
	}

	select {
	case <-m.stopper:
		close(m.events)
		close(m.done)
		return false
	case m.events <- &MultiConsumerEvent{Err: err, Topic: topic, Partition: partition}:
		return true
	}
}

// Register a given topic-partition with the multiconsumer, beginning at the
// determined offset. If the topic-partition is already registered with the
// multiconsumer, this method has no effect. To specify an offset manually, give
// the offset as `offset` and pass `OffsetMethodManual` for the last parameter.
// If you pass `OffsetMethodEarliest` or `OffsetMethodLatest`, the `offset`
// parameter is ignored, and you should generally pass -1.
func (m *MultiConsumer) AddTopicPartition(topic string, partition int32, offset int64, offsetMethod OffsetMethod) (err error) {

	switch offsetMethod {
	case OffsetMethodManual:
		if offset < 0 {
			return ConfigurationError("OffsetValue cannot be < 0 when OffsetMethod is MANUAL")
		}
	case OffsetMethodNewest:
		offset, err = m.getOffset(topic, partition, LatestOffsets, true)
		if err != nil {
			return err
		}
	case OffsetMethodOldest:
		offset, err = m.getOffset(topic, partition, EarliestOffset, true)
		if err != nil {
			return err
		}
	default:
		return ConfigurationError("Invalid OffsetMethod")
	}

	m.m.Lock()
	if _, ok := m.topicPartitions[topic]; !ok {
		m.topicPartitions[topic] = make(map[int32]*topicPartition)
	}
	if _, ok := m.topicPartitions[topic][partition]; !ok {
		tp := &topicPartition{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
			FetchSize: m.config.DefaultFetchSize,
		}
		m.topicPartitions[topic][partition] = tp
		m.waiting <- tp
	}
	m.m.Unlock()

	return nil
}

func (m *MultiConsumer) allocator() {
	for tp := range m.waiting {
		err := m.client.RefreshTopicMetadata(tp.Topic)
		if err != nil {
			m.sendError("", -1, err)
			m.waiting <- tp // good idea? or should we discard it?
			continue
		}
		broker, err := m.client.Leader(tp.Topic, tp.Partition)
		if err != nil {
			m.sendError("", -1, err)
			m.waiting <- tp // good idea? or should we discard it?
			continue
		}

		m.m.Lock()
		br, ok := m.brokerRunners[broker]
		if !ok {
			br = &brokerRunner{intake: make(chan *topicPartition, 8)}
			m.brokerRunners[broker] = br
			go m.brokerRunner(broker)
		}
		br.intake <- tp
		m.m.Unlock()
	}
}

// Events returns the read channel for any events (messages or errors) that
// might be returned by the broker.
func (m *MultiConsumer) Events() <-chan *MultiConsumerEvent {
	return m.events
}

func (m *MultiConsumer) brokerRunner(broker *Broker) {
	m.m.RLock()
	br := m.brokerRunners[broker]
	m.m.RUnlock()

	defer m.cleanupBrokerRunner(br)

	var tps []*topicPartition
	for {
		select {
		case tp := <-br.intake:
			br.m.Lock()
			tps = append(tps, tp)
			br.m.Unlock()
		default:
			if len(tps) == 0 {
				return
			}
			tps = m.fetchMessages(broker, tps)
		}
	}
}

func (m *MultiConsumer) fetchMessages(broker *Broker, tps []*topicPartition) []*topicPartition {
	request := new(FetchRequest)
	request.MinBytes = m.config.MinFetchSize
	request.MaxWaitTime = m.config.MaxWaitTime
	for topic, data := range m.topicPartitions {
		for partition, tp := range data {
			request.AddBlock(topic, partition, tp.Offset, tp.FetchSize)
		}
	}

	response, err := broker.Fetch(m.client.id, request)
	switch {
	case err == nil:
		break
	case err == EncodingError:
		m.sendError("", -1, err)
		return tps
	default:
		m.client.disconnectBroker(broker)
		for _, tp := range tps {
			m.waiting <- tp
		}
		return nil
	}

tploop:
	for _, tp := range tps {
		block := response.GetBlock(tp.Topic, tp.Partition)
		if block == nil {
			m.sendError(tp.Topic, tp.Partition, IncompleteResponse)
			continue
		}

		switch block.Err {
		case NoError:
		case UnknownTopicOrPartition, NotLeaderForPartition, LeaderNotAvailable:
			oldTps := tps
			tps = []*topicPartition{}

			// tps.reject! { |otp| otp == tp }
			for _, otp := range oldTps {
				if otp != tp {
					tps = append(tps, otp)
				}
			}

			m.waiting <- tp
			continue tploop
		default:
			m.sendError(tp.Topic, tp.Partition, block.Err)
			continue tploop
		}

		if len(block.MsgSet.Messages) == 0 {
			if block.MsgSet.PartialTrailingMessage {
				if m.config.MaxMessageSize == 0 {
					tp.FetchSize *= 2
				} else {
					if tp.FetchSize == m.config.MaxMessageSize {
						m.sendError(tp.Topic, tp.Partition, MessageTooLarge)
						continue
					} else {
						tp.FetchSize *= 2
						if tp.FetchSize > m.config.MaxMessageSize {
							tp.FetchSize = m.config.MaxMessageSize
						}
					}
				}
			}
		} else {
			tp.FetchSize = m.config.DefaultFetchSize
		}

		for _, msgBlock := range block.MsgSet.Messages {
			select {
			case <-m.stopper:
				close(m.events)
				close(m.done)
				return nil
			case m.events <- &MultiConsumerEvent{
				Topic:     tp.Topic,
				Partition: tp.Partition,
				Key:       msgBlock.Msg.Key,
				Value:     msgBlock.Msg.Value,
				Offset:    msgBlock.Offset,
			}:
				tp.Offset++
			}
		}

	}

	return tps
}

func (m *MultiConsumer) cleanupBrokerRunner(br *brokerRunner) {
	m.m.Lock()
	close(br.intake)
	delete(m.brokerRunners, br.Broker)
	m.m.Unlock()
}

func (m *MultiConsumer) getOffset(topic string, partition int32, where OffsetTime, retry bool) (int64, error) {
	request := &OffsetRequest{}
	request.AddBlock(topic, partition, where, 1)

	broker, err := m.client.Leader(topic, partition)
	if err != nil {
		return -1, err
	}

	response, err := broker.GetAvailableOffsets(m.client.id, request)
	switch err {
	case nil:
		break
	case EncodingError:
		return -1, err
	default:
		if !retry {
			return -1, err
		}
		m.client.disconnectBroker(broker)
		broker, err = m.client.Leader(topic, partition)
		if err != nil {
			return -1, err
		}
		return m.getOffset(topic, partition, where, false)
	}

	block := response.GetBlock(topic, partition)
	if block == nil {
		return -1, IncompleteResponse
	}

	switch block.Err {
	case NoError:
		if len(block.Offsets) < 1 {
			return -1, IncompleteResponse
		}
		return block.Offsets[0], nil
	case UnknownTopicOrPartition, NotLeaderForPartition, LeaderNotAvailable:
		if !retry {
			return -1, block.Err
		}
		err = m.client.RefreshTopicMetadata(topic)
		if err != nil {
			return -1, err
		}
		broker, err = m.client.Leader(topic, partition)
		if err != nil {
			return -1, err
		}
		return m.getOffset(topic, partition, where, false)
	}

	return -1, block.Err
}
