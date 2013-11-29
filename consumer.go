package sarama

import (
	"errors"
	"sync"

	"github.com/Sirupsen/tomb"
)

// OffsetMethod is passed in ConsumerConfig to tell the consumer how to determine the starting offset.
type OffsetMethod int

const (
	// OffsetMethodManual causes the consumer to interpret the OffsetValue in the ConsumerConfig as the
	// offset at which to start, allowing the user to manually specify their desired starting offset.
	OffsetMethodManual OffsetMethod = iota
	// OffsetMethodNewest causes the consumer to start at the most recent available offset, as
	// determined by querying the broker.
	OffsetMethodNewest
	// OffsetMethodOldest causes the consumer to start at the oldest available offset, as
	// determined by querying the broker.
	OffsetMethodOldest
)

// ConsumerConfig is used to pass multiple configuration options to NewConsumer.
type ConsumerConfig struct {
	// The default (maximum) amount of data to fetch from the broker in each request. The default of 0 is treated as 1024 bytes.
	DefaultFetchSize int32
	// The minimum amount of data to fetch in a request - the broker will wait until at least this many bytes are available.
	// The default of 0 is treated as 'at least one' to prevent the consumer from spinning when no messages are available.
	MinFetchSize int32
	// The maximum permittable message size - messages larger than this will return MessageTooLarge. The default of 0 is
	// treated as no limit.
	MaxMessageSize int32
	// The maximum amount of time (in ms) the broker will wait for MinFetchSize bytes to become available before it
	// returns fewer than that anyways. The default of 0 causes Kafka to return immediately, which is rarely desirable
	// as it causes the Consumer to spin when no events are available. 100-500ms is a reasonable range for most cases.
	MaxWaitTime int32

	// The number of events to buffer in the Events channel. Setting this can let the
	// consumer continue fetching messages in the background while local code consumes events,
	// greatly improving throughput.
	EventBufferSize int
}

// Consumer processes Kafka messages from an arbitrarily large set of Kafka
// topic-partitions. You MUST call Close() on a Consumer to avoid leaks; it
// will not be garbage-collected otherwise.
//
// It is STRONGLY recommended to pass MinFetchSize and MaxWaitTime options via
// the ConsumerConfig parameter to avoid excessive network traffic. Sensible
// defaults are 50*1024 and 1000, respsectively.
type Consumer struct {
	m sync.RWMutex

	client *Client

	group  string
	config ConsumerConfig

	brokerRunners map[*Broker]*brokerRunner
	consumerTPs   map[topicPartition]*consumerTP

	waiting chan *consumerTP
	tomb    tomb.Tomb
	wg      sync.WaitGroup
	events  chan *ConsumerEvent
}

// ConsumerEvent type is a sort of Either monad; it contains a Key+Value or
// an error. If Key+Value+Offset is given, it will always contain a
// Topic+Partition. In the error case, it will contain Topic+Partition if
// relevant; otherwise they will be ("",-1).
type ConsumerEvent struct {
	Topic      string
	Partition  int32
	Key, Value []byte
	Offset     int64
	Err        error
}

type brokerRunner struct {
	m      sync.Mutex
	intake chan *consumerTP
	Broker *Broker
}

type consumerTP struct {
	Topic     string
	Partition int32
	Offset    int64
	FetchSize int32
}

// NewConsumer creates a new multiconsumer attached to a given client. It
// will read messages as a member of the given consumer group.
func NewConsumer(client *Client, group string, config *ConsumerConfig) (*Consumer, error) {
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

	m := &Consumer{
		client:        client,
		group:         group,
		config:        *config,
		brokerRunners: make(map[*Broker]*brokerRunner),
		waiting:       make(chan *consumerTP, 16),
		consumerTPs:   make(map[topicPartition]*consumerTP),
		events:        make(chan *ConsumerEvent, config.EventBufferSize),
	}

	go withRecover(m.allocator)

	return m, nil
}

// Close stops the multiconsumer from fetching messages. It is required to call
// this function before a multiconsumer object passes out of scope, as it will
// otherwise leak memory. You must call this before calling Close on the
// underlying client.
func (m *Consumer) Close() error {
	m.tomb.Kill(errors.New("Close() called"))
	return m.tomb.Wait()
}

func (m *Consumer) sendError(topic string, partition int32, err error) bool {
	if err == nil {
		return true
	}

	select {
	case <-m.tomb.Dying():
		return false
	case m.events <- &ConsumerEvent{Err: err, Topic: topic, Partition: partition}:
		return true
	}
}

// Register a given topic-partition with the multiconsumer, beginning at the
// determined offset. If the topic-partition is already registered with the
// multiconsumer, this method has no effect. To specify an offset manually, give
// the offset as `offset` and pass `OffsetMethodManual` for the last parameter.
// If you pass `OffsetMethodEarliest` or `OffsetMethodLatest`, the `offset`
// parameter is ignored, and you should generally pass -1.
func (m *Consumer) AddTopicPartition(topic string, partition int32, offsetMethod OffsetMethod, offset int64) (err error) {

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

	tp := topicPartition{topic, partition}
	m.m.Lock()
	if _, ok := m.consumerTPs[tp]; !ok {
		ctp := &consumerTP{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
			FetchSize: m.config.DefaultFetchSize,
		}
		m.consumerTPs[tp] = ctp
		m.waiting <- ctp
	}
	m.m.Unlock()

	return nil
}

func (m *Consumer) allocator() {
	for {
		select {
		case <-m.tomb.Dying():
			// interrupt all brokers so that any currently-blocking FetchReqeusts are interrupted.
			// This is pretty hackish, but it does the trick.
			for broker := range m.brokerRunners {
				if broker.conn != nil {
					broker.conn.Close()
				}
			}
			m.wg.Wait()
			m.tomb.Done()
			return
		case tp := <-m.waiting:
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
				br = &brokerRunner{intake: make(chan *consumerTP, 8)}
				m.brokerRunners[broker] = br
				m.wg.Add(1)
				go withRecover(func() { m.brokerRunner(broker) })
			}
			br.intake <- tp
			m.m.Unlock()
		}
	}
}

// Events returns the read channel for any events (messages or errors) that
// might be returned by the broker.
func (m *Consumer) Events() <-chan *ConsumerEvent {
	return m.events
}

func (m *Consumer) brokerRunner(broker *Broker) {
	defer m.wg.Done()
	m.m.RLock()
	br := m.brokerRunners[broker]
	m.m.RUnlock()

	defer m.cleanupBrokerRunner(br)

	var tps []*consumerTP
	for {
		select {
		case <-m.tomb.Dying():
			return
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

func (m *Consumer) fetchMessages(broker *Broker, tps []*consumerTP) []*consumerTP {
	request := new(FetchRequest)
	request.MinBytes = m.config.MinFetchSize
	request.MaxWaitTime = m.config.MaxWaitTime
	for _, tp := range tps {
		request.AddBlock(tp.Topic, tp.Partition, tp.Offset, tp.FetchSize)
	}

	response, err := broker.Fetch(m.client.id, request)

	select {
	case <-m.tomb.Dying():
		return nil
	default:
	}

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
			tps = []*consumerTP{}

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
			case <-m.tomb.Dying():
				return nil
			case m.events <- &ConsumerEvent{
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

func (m *Consumer) cleanupBrokerRunner(br *brokerRunner) {
	m.m.Lock()
	close(br.intake)
	delete(m.brokerRunners, br.Broker)
	m.m.Unlock()
}

func (m *Consumer) getOffset(topic string, partition int32, where OffsetTime, retry bool) (int64, error) {
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

// really just for testing.
func (c *Consumer) offset(topic string, partition int32) int64 {
	return c.consumerTPs[topicPartition{topic, partition}].Offset
}
