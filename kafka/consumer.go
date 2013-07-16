package kafka

// Consumer processes Kafka messages from a given topic and partition.
// You MUST call Close() on a consumer to avoid leaks, it will not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
type Consumer struct {
	client    *Client
	topic     string
	partition int32
	group     string
	offset    int64
}

// NewConsumer creates a new consumer attached to the given client. It will read messages from the given topic and partition, as
// part of the named consumer group.
func NewConsumer(client *Client, topic string, partition int32, group string) *Consumer {
	c := new(Consumer)
	c.client = client
	c.topic = topic
	c.partition = partition
	c.group = group

	// We should really be sending an OffsetFetchRequest, but that doesn't seem to
	// work in kafka yet. Hopefully will in beta 2...
	c.offset = 0

	return c
}

func (c *Consumer) Close() {
}
