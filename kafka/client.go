package kafka

import k "sarama/protocol"

// Client is a generic Kafka client. It manages connections to one or more Kafka brokers.
// You MUST call Close() on a client to avoid leaks, it will not be garbage-collected
// automatically when it passes out of scope. A single client can be safely shared by
// multiple concurrent Producers and Consumers.
type Client struct {
	id    string
	cache *metadataCache
}

// NewClient creates a new Client with the given client ID. It connects to the broker at the given
// host:port address, and uses that broker to automatically fetch metadata on the rest of the kafka cluster.
// If metadata cannot be retrieved (even if the connection otherwise succeeds) then the client is not created.
func NewClient(id string, host string, port int32) (client *Client, err error) {
	client = new(Client)
	client.id = id
	client.cache, err = newMetadataCache(client, host, port)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Close shuts down all broker connections managed by this client. It is required to call this function before
// a client object passes out of scope, as it will otherwise leak memory. You must close any Producers or Consumers
// using a client before you close the client.
func (client *Client) Close() {
	client.cache.closeAll()
}

func (client *Client) leader(topic string, partition_id int32) (*k.Broker, error) {
	leader := client.cache.leader(topic, partition_id)

	if leader == nil {
		err := client.cache.refreshTopic(topic)
		if err != nil {
			return nil, err
		}

		leader = client.cache.leader(topic, partition_id)
	}

	if leader == nil {
		return nil, k.UNKNOWN_TOPIC_OR_PARTITION
	}

	return leader, nil
}

func (client *Client) partitions(topic string) ([]int32, error) {
	partitions := client.cache.partitions(topic)

	if partitions == nil {
		err := client.cache.refreshTopic(topic)
		if err != nil {
			return nil, err
		}

		partitions = client.cache.partitions(topic)
	}

	if partitions == nil {
		return nil, NoSuchTopic
	}

	return partitions, nil
}
