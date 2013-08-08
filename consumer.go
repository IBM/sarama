package sarama

type MessageOrError struct {
	MessageBlock
	Err error
}

// Consumer processes Kafka messages from a given topic and partition.
// You MUST call Close() on a consumer to avoid leaks, it will not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
type Consumer struct {
	client *Client

	topic     string
	partition int32
	group     string

	offset        int64
	broker        *Broker
	stopper, done chan bool
	messages      chan *MessageOrError
}

// NewConsumer creates a new consumer attached to the given client. It will read messages from the given topic and partition, as
// part of the named consumer group.
func NewConsumer(client *Client, topic string, partition int, group string) (*Consumer, error) {
	broker, err := client.leader(topic, int32(partition))
	if err != nil {
		return nil, err
	}

	c := new(Consumer)
	c.client = client
	c.topic = topic
	c.partition = int32(partition)
	c.group = group

	// We should really be sending an OffsetFetchRequest, but that doesn't seem to
	// work in kafka yet. Hopefully will in beta 2...
	c.offset = 0
	c.broker = broker
	c.stopper = make(chan bool)
	c.done = make(chan bool)
	c.messages = make(chan *MessageOrError)

	go c.fetchMessages()

	return c, nil
}

// Messages returns the read channel for all messages that will be returned by the broker.
func (c *Consumer) Messages() <-chan *MessageOrError {
	return c.messages
}

// Close stops the consumer from fetching messages. It is required to call this function before
// a consumer object passes out of scope, as it will otherwise leak memory. You must call this before
// calling Close on the underlying client.
func (c *Consumer) Close() {
	close(c.stopper)
	<-c.done
}

// helper function for safely sending an error
// if it returns true, the error was sent (or was nil)
// if it returns false, the stopper channel signaled that your goroutine should return!
func (c *Consumer) sendError(err error) bool {
	if err == nil {
		return true
	}

	select {
	case <-c.stopper:
		close(c.messages)
		close(c.done)
		return false
	case c.messages <- &MessageOrError{Err: err}:
		return true
	}

	return true
}

func (c *Consumer) fetchMessages() {

	var fetchSize int32 = 1024

	for {
		request := new(FetchRequest)
		request.MinBytes = 1
		request.MaxWaitTime = 1000
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
				fetchSize *= 2
			}
			select {
			case <-c.stopper:
				close(c.messages)
				close(c.done)
				return
			default:
				continue
			}
		}

		for _, msgBlock := range block.MsgSet.Messages {
			select {
			case <-c.stopper:
				close(c.messages)
				close(c.done)
				return
			case c.messages <- &MessageOrError{*msgBlock, nil}:
				c.offset++
			}
		}
	}
}
