package kafka

type Client struct {
	id      *string
	manager *brokerManager
}

func NewClient(id *string, host string, port int32) (client *Client, err error) {
	client = new(Client)
	client.id = id
	client.manager, err = newBrokerManager(client, host, port)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (c *Client) todo(broker *broker, body requestBody, res response) error {
	// the correlation id gets filled in by the broker
	req := request{0, c.id, body}
	gotResponse, err := broker.sendAndReceive(req, res);
	switch err.(type) {
	case EncodingError:
		return err
	case nil:
		// no error, did we get a response?
		if !gotResponse {
			return nil
		}
	default:
		// broker error, so discard that broker
		// TODO c.manager.terminateBroker(b.id)
		return err
	}

        // we successfully got and parsed a response, so check for stale brokers and other errors
	toRetry := make(map[*string]map[int32]bool)
	for _, topic := range res.topics() {
		for _, partition := range topic.partitions() {
			switch partition.err() {
			case NO_ERROR:
				continue
			case UNKNOWN_TOPIC_OR_PARTITION, NOT_LEADER_FOR_PARTITION:
				if toRetry[topic.name()] == nil {
					toRetry[topic.name()] = make(map[int32]bool)
				}
				toRetry[topic.name()][partition.id()] = true
			default:
				return partition.err()
			}
		}
	}

	if len(toRetry) == 0 {
		return nil
	}

	// refresh necessary metadata
	toRefresh := make([]*string, len(toRetry))
	for name, _ := range toRetry {
		toRefresh = append(toRefresh, name)
	}
	// TODO c.manager.refreshTopics(toRefresh)

	// now retry the request chunks that failed
	for _, topic := range body.topics() {
		if toRetry[topic.name()] != nil {
			for _, partition := range topic.partitions() {
				if toRetry[topic.name()][partition.id()] {
				}
			}
		}
	}

	return nil
}
