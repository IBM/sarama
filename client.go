package kafka

type API struct {
	key     int16
	version int16
}

var (
	REQUEST_PRODUCE        = API{0, 0}
	REQUEST_FETCH          = API{1, 0}
	REQUEST_OFFSET         = API{2, 0}
	REQUEST_METADATA       = API{3, 0}
	REQUEST_LEADER_AND_ISR = API{4, 0}
	REQUEST_STOP_REPLICA   = API{5, 0}
	REQUEST_OFFSET_COMMIT  = API{6, 0}
	REQUEST_OFFSET_FETCH   = API{7, 0}
)

type Client struct {
	id      *string
	brokers *brokerManager
}

func NewClient(id *string, host string, port int32) (client *Client, err error) {
	client = new(Client)
	client.id = id
	client.brokers, err = newBrokerManager(host, port)
	return client, err
}
