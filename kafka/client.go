package kafka

import k "sarama/protocol"

type Client struct {
	id    *string
	cache *metadataCache
}

func NewClient(id *string, host string, port int32) (client *Client, err error) {
	client = new(Client)
	client.id = id
	client.cache, err = newMetadataCache(client, host, port)
	if err != nil {
		return nil, err
	}
	return client, nil
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
