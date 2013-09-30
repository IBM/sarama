package sarama

import (
	"sync"
)

type brokerRunner struct {
	m      sync.Mutex
	intake chan *topicPartition
	Broker *Broker
}

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

type MultiConsumerEvent struct {
	Topic      string
	Partition  int32
	Key, Value []byte
	Offset     int64
	Err        error
}

type topicPartition struct {
	Topic     string
	Partition int32
	Offset    int64
}

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
		client:        client,
		group:         group,
		config:        *config,
		brokerRunners: make(map[*Broker]*brokerRunner),
		waiting:       make(chan *topicPartition),
		stopper:       make(chan bool),
		done:          make(chan bool),
		events:        make(chan *MultiConsumerEvent, config.EventBufferSize),
	}

	go m.allocator()

	return m, nil
}

func (m *MultiConsumer) Close() {

}

func (m *MultiConsumer) AddTopicPartition(topic string, partition int32, offset int64) {
	m.waiting <- &topicPartition{Topic: topic, Partition: partition, Offset: offset}
}

func (m *MultiConsumer) Events() {

}

func (m *MultiConsumer) allocator() {
	for tp := range m.waiting {
		// should we refresh metadata before this?
		broker, err := m.client.Leader(tp.Topic, tp.Partition)
		if err != nil {
			panic(err)
		}

		m.m.Lock()
		br, ok := m.brokerRunners[broker]
		if !ok {
			br = &brokerRunner{intake: make(chan *topicPartition, 8)}
			m.brokerRunners[broker] = br
			br.intake <- tp
			go m.brokerRunner(broker)
		}
		m.m.Unlock()
	}
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
	fetchSize := m.config.DefaultFetchSize

	request := new(FetchRequest)
	request.MinBytes = m.config.MinFetchSize
	request.MaxWaitTime = m.config.MaxWaitTime
	for topic, data := range m.topicPartitions {
		for partition, tp := range data {
			request.AddBlock(topic, partition, tp.Offset, fetchSize)
		}
	}

	response, err := broker.Fetch(m.client.id, request)
	switch {
	case err == nil:
		break
	case err == EncodingError:
		// TODO
	default:
		// TODO
	}

	for _, tp := range tps {
		block := response.GetBlock(tp.Topic, tp.Partition)
		if block == nil {
			panic("block is nil")
		}

		switch block.Err {
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
		default:
			// send to client
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
