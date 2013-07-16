package kafka

import k "sarama/protocol"

// Consumer processes Kafka messages from a given topic and partition.
// You MUST call Close() on a consumer to avoid leaks, it will not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
type Consumer struct {
	client *Client

	topic     string
	partition int32
	group     string

	offset        int64
	broker        *k.Broker
	stopper, done chan bool
	messages      chan *Message
	errors        chan error
}

// NewConsumer creates a new consumer attached to the given client. It will read messages from the given topic and partition, as
// part of the named consumer group.
func NewConsumer(client *Client, topic string, partition int32, group string) (*Consumer, error) {
	broker, err := client.leader(topic, partition)
	if err != nil {
		return nil, err
	}

	c := new(Consumer)
	c.client = client
	c.topic = topic
	c.partition = partition
	c.group = group

	// We should really be sending an OffsetFetchRequest, but that doesn't seem to
	// work in kafka yet. Hopefully will in beta 2...
	c.offset = 0
	c.broker = broker
	c.stopper = make(chan bool)
	c.done = make(chan bool)
	c.messages = make(chan *Message)
	c.errors = make(chan error)

	go c.fetchMessages()

	return c, nil
}

// Errors returns the read channel for any errors that might be returned by the broker.
func (c *Consumer) Errors() <-chan error {
	return c.errors
}

// Messages returns the read channel for all messages that will be returned by the broker.
func (c *Consumer) Messages() <-chan *Message {
	return c.messages
}

// Close stops the consumer from fetching messages. It is required to call this function before
// a consumer object passes out of scope, as it will otherwise leak memory. You must call this before
// calling Close on the underlying client.
func (c *Consumer) Close() {
	close(c.stopper)
	<-c.done
}

func (c *Consumer) fetchMessages() {
	for {
		request := new(k.FetchRequest)
		request.MinBytes = 1
		request.MaxWaitTime = 10000
		request.AddBlock(c.topic, c.partition, c.offset, 1024)

		response, err := c.broker.Fetch(c.client.id, request)
		if err != nil {
			select {
			case <-c.stopper:
				close(c.messages)
				close(c.errors)
				close(c.done)
				return
			case c.errors <- err:
				continue
			}
		}

		block := response.GetBlock(c.topic, c.partition)
		if block == nil {
			select {
			case <-c.stopper:
				close(c.messages)
				close(c.errors)
				close(c.done)
				return
			case c.errors <- IncompleteResponse:
				continue
			}
		}

		if block.Err != k.NO_ERROR {
			select {
			case <-c.stopper:
				close(c.messages)
				close(c.errors)
				close(c.done)
				return
			case c.errors <- block.Err:
				continue
			}
		}

		if len(block.MsgSet.Messages) == 0 {
			panic("What should we do here?")
			/*
				If we timed out waiting for a new message we should just poll again. However, if we got part of a message but
				our maxBytes was too small (hard-coded 1024 at the moment) we should increase that and ask again. If we just poll
				with the same size immediately we'll end up in an infinite loop DOSing the broker...
			*/
		}

		for _, msgBlock := range block.MsgSet.Messages {
			// smoosh the kafka return data into a more useful single struct
			msg := new(Message)
			msg.Offset = msgBlock.Offset
			msg.Key = msgBlock.Msg.Key
			msg.Value = msgBlock.Msg.Value

			// and send it
			select {
			case <-c.stopper:
				close(c.messages)
				close(c.errors)
				close(c.done)
				return
			case c.messages <- msg:
				c.offset++
			}
		}
	}
}
