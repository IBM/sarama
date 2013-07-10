package kafka

type Client struct {
	id      *string
	brokers *brokerManager
}

func NewClient(id *string, host string, port int32) (client *Client, err error) {
	client = new(Client)
	client.id = id
	client.brokers, err = newBrokerManager(host, port)
	if err != nil {
		return nil, err
	}
	return client, nil
}
