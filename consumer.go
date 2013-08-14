package sarama

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
	messages      chan *MessageBlock
	errors        chan error
}

// NewConsumer creates a new consumer attached to the given client. It will read messages from the given topic and partition, as
// part of the named consumer group.
func NewConsumer(client *Client, topic string, partition int32, group string, config ConsumerConfig) (*Consumer, error) {
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

	broker, err := client.leader(topic, partition)
	if err != nil {
		return nil, err
	}

	c := new(Consumer)
	c.client = client
	c.topic = topic
	c.partition = partition
	c.group = group
	c.config = config

	// We should really be sending an OffsetFetchRequest, but that doesn't seem to
	// work in kafka yet. Hopefully will in beta 2...
	c.offset = 0
	c.broker = broker
	c.stopper = make(chan bool)
	c.done = make(chan bool)
	c.messages = make(chan *MessageBlock)
	c.errors = make(chan error)

	go c.fetchMessages()

	return c, nil
}

// Errors returns the read channel for any errors that might be returned by the broker.
func (c *Consumer) Errors() <-chan error {
	return c.errors
}

// Messages returns the read channel for all messages that will be returned by the broker.
func (c *Consumer) Messages() <-chan *MessageBlock {
	return c.messages
}

// Close stops the consumer from fetching messages. It is required to call this function before
// a consumer object passes out of scope, as it will otherwise leak memory. You must call this before
// calling Close on the underlying client.
func (c *Consumer) Close() {
	close(c.stopper)
	<-c.done
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
		close(c.messages)
		close(c.errors)
		close(c.done)
		return false
	case c.errors <- err:
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
			for c.broker = nil; err != nil; c.broker, err = c.client.leader(c.topic, c.partition) {
				if !c.sendError(err) {
					return
				}
			}
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
			err = c.client.refreshTopic(c.topic)
			if c.sendError(err) {
				for c.broker = nil; err != nil; c.broker, err = c.client.leader(c.topic, c.partition) {
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
				close(c.messages)
				close(c.errors)
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
				close(c.messages)
				close(c.errors)
				close(c.done)
				return
			case c.messages <- msgBlock:
				c.offset++
			}
		}
	}
}
