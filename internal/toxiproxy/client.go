package toxiproxy

type Client struct{}

func NewClient(endpoint string) *Client {
	return &Client{}
}

func (c *Client) Proxy(name string) (*Proxy, error) {
	return &Proxy{}, nil
}

func (c *Client) CreateProxy(name string, listenerAddr string, targetAddr string) (*Proxy, error) {
	return &Proxy{}, nil
}

func (c *Client) ResetState() error {
	return nil
}
