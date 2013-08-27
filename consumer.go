package sarama

// OffsetMethod is passed in ConsumerConfig to tell the consumer how to determine the starting offset.
type OffsetMethod int

const (
	// OFFSET_METHOD_MANUAL causes the consumer to interpret the OffsetValue in the ConsumerConfig as the
	// offset at which to start, allowing the user to manually specify their desired starting offset.
	OFFSET_METHOD_MANUAL OffsetMethod = iota
	// OFFSET_METHOD_NEWEST causes the consumer to start at the most recent available offset, as
	// determined by querying the broker.
	OFFSET_METHOD_NEWEST
	// OFFSET_METHOD_OLDEST causes the consumer to start at the oldest available offset, as
	// determined by querying the broker.
	OFFSET_METHOD_OLDEST
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
	// returns fewer than that anyways. The default of 0 is treated as no limit.
	MaxWaitTime int32

	// The method used to determine at which offset to begin consuming messages.
	OffsetMethod OffsetMethod
	// Interpreted differently according to the value of OffsetMethod.
	OffsetValue int64

	// The number of events to buffer in the Events channel. Setting this can let the
	// consumer continue fetching messages in the background while local code consumes events,
	// greatly improving throughput.
	EventBufferSize int
}

// ConsumerEvent is what is provided to the user when an event occurs. It is either an error (in which case Err is non-nil) or
// a message (in which case Err is nil and the other fields are all set).
type ConsumerEvent struct {
	Key, Value []byte
	Offset     int64
	Err        error
}

// Consumer processes Kafka messages from a given topic and partition.
// You MUST call Close() on a consumer to avoid leaks, it will not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
type Consumer struct {
	client *Client

	topic     string
	partition int32
	group     string
	config    ConsumerConfig

	offset        int64
	broker        *Broker
	stopper, done chan bool
	events        chan *ConsumerEvent
}

// NewConsumer creates a new consumer attached to the given client. It will read messages from the given topic and partition, as
// part of the named consumer group.
func NewConsumer(client *Client, topic string, partition int32, group string, config *ConsumerConfig) (*Consumer, error) {
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

	broker, err := client.Leader(topic, partition)
	if err != nil {
		return nil, err
	}

	c := new(Consumer)
	c.client = client
	c.topic = topic
	c.partition = partition
	c.group = group
	c.config = *config
	c.broker = broker

	switch config.OffsetMethod {
	case OFFSET_METHOD_MANUAL:
		if config.OffsetValue < 0 {
			return nil, ConfigurationError("OffsetValue cannot be < 0 when OffsetMethod is MANUAL")
		}
		c.offset = config.OffsetValue
	case OFFSET_METHOD_NEWEST:
		c.offset, err = c.getOffset(LATEST_OFFSETS, true)
		if err != nil {
			return nil, err
		}
	case OFFSET_METHOD_OLDEST:
		c.offset, err = c.getOffset(EARLIEST_OFFSET, true)
		if err != nil {
			return nil, err
		}
	default:
		return nil, ConfigurationError("Invalid OffsetMethod")
	}

	c.stopper = make(chan bool)
	c.done = make(chan bool)
	c.events = make(chan *ConsumerEvent, config.EventBufferSize)

	go c.fetchMessages()

	return c, nil
}

// Events returns the read channel for any events (messages or errors) that might be returned by the broker.
func (c *Consumer) Events() <-chan *ConsumerEvent {
	return c.events
}

// Close stops the consumer from fetching messages. It is required to call this function before
// a consumer object passes out of scope, as it will otherwise leak memory. You must call this before
// calling Close on the underlying client.
func (c *Consumer) Close() error {
	close(c.stopper)
	<-c.done
	return nil
}

// helper function for safely sending an error on the errors channel
// if it returns true, the error was sent (or was nil)
// if it returns false, the stopper channel signaled that your goroutine should return!
func (c *Consumer) sendError(err error) bool {
	if err == nil {
		return true
	}

	select {
	case <-c.stopper:
		close(c.events)
		close(c.done)
		return false
	case c.events <- &ConsumerEvent{Err: err}:
		return true
	}

	return true
}

func (c *Consumer) fetchMessages() {

	var fetchSize int32 = c.config.DefaultFetchSize

	for {
		request := new(FetchRequest)
		request.MinBytes = c.config.MinFetchSize
		request.MaxWaitTime = c.config.MaxWaitTime
		request.AddBlock(c.topic, c.partition, c.offset, fetchSize)

		response, err := c.broker.Fetch(c.client.id, request)
		switch {
		case err == nil:
			break
		case err == EncodingError:
			if c.sendError(err) {
				continue
			} else {
				return
			}
		default:
			c.client.disconnectBroker(c.broker)
			for c.broker = nil; err != nil; c.broker, err = c.client.Leader(c.topic, c.partition) {
				if !c.sendError(err) {
					return
				}
			}
			continue
		}

		block := response.GetBlock(c.topic, c.partition)
		if block == nil {
			if c.sendError(IncompleteResponse) {
				continue
			} else {
				return
			}
		}

		switch block.Err {
		case NO_ERROR:
			break
		case UNKNOWN_TOPIC_OR_PARTITION, NOT_LEADER_FOR_PARTITION, LEADER_NOT_AVAILABLE:
			err = c.client.RefreshTopicMetadata(c.topic)
			if c.sendError(err) {
				for c.broker = nil; err != nil; c.broker, err = c.client.Leader(c.topic, c.partition) {
					if !c.sendError(err) {
						return
					}
				}
				continue
			} else {
				return
			}
		default:
			if c.sendError(block.Err) {
				continue
			} else {
				return
			}
		}

		if len(block.MsgSet.Messages) == 0 {
			// We got no messages. If we got a trailing one then we need to ask for more data.
			// Otherwise we just poll again and wait for one to be produced...
			if block.MsgSet.PartialTrailingMessage {
				if c.config.MaxMessageSize == 0 {
					fetchSize *= 2
				} else {
					if fetchSize == c.config.MaxMessageSize {
						if c.sendError(MessageTooLarge) {
							continue
						} else {
							return
						}
					} else {
						fetchSize *= 2
						if fetchSize > c.config.MaxMessageSize {
							fetchSize = c.config.MaxMessageSize
						}
					}
				}
			}
			select {
			case <-c.stopper:
				close(c.events)
				close(c.done)
				return
			default:
				continue
			}
		} else {
			fetchSize = c.config.DefaultFetchSize
		}

		for _, msgBlock := range block.MsgSet.Messages {
			select {
			case <-c.stopper:
				close(c.events)
				close(c.done)
				return
			case c.events <- &ConsumerEvent{Key: msgBlock.Msg.Key, Value: msgBlock.Msg.Value, Offset: msgBlock.Offset}:
				c.offset++
			}
		}
	}
}

func (c *Consumer) getOffset(where OffsetTime, retry bool) (int64, error) {
	request := &OffsetRequest{}
	request.AddBlock(c.topic, c.partition, where, 1)

	response, err := c.broker.GetAvailableOffsets(c.client.id, request)
	switch err {
	case nil:
		break
	case EncodingError:
		return -1, err
	default:
		if !retry {
			return -1, err
		}
		c.client.disconnectBroker(c.broker)
		c.broker, err = c.client.Leader(c.topic, c.partition)
		if err != nil {
			return -1, err
		}
		return c.getOffset(where, false)
	}

	block := response.GetBlock(c.topic, c.partition)
	if block == nil {
		return -1, IncompleteResponse
	}

	switch block.Err {
	case NO_ERROR:
		if len(block.Offsets) < 1 {
			return -1, IncompleteResponse
		}
		return block.Offsets[0], nil
	case UNKNOWN_TOPIC_OR_PARTITION, NOT_LEADER_FOR_PARTITION, LEADER_NOT_AVAILABLE:
		if !retry {
			return -1, block.Err
		}
		err = c.client.RefreshTopicMetadata(c.topic)
		if err != nil {
			return -1, err
		}
		c.broker, err = c.client.Leader(c.topic, c.partition)
		if err != nil {
			return -1, err
		}
		return c.getOffset(where, false)
	}

	return -1, block.Err
}
