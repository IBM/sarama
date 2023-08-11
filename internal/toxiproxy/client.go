package toxiproxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

type Client struct {
	httpClient *http.Client
	endpoint   string
}

func NewClient(endpoint string) *Client {
	return &Client{
		httpClient: &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          -1,
				DisableKeepAlives:     true,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		},
		endpoint: endpoint,
	}
}

func (c *Client) CreateProxy(
	name string,
	listenAddr string,
	targetAddr string,
) (*Proxy, error) {
	proxy := &Proxy{
		Name:       name,
		ListenAddr: listenAddr,
		TargetAddr: targetAddr,
		Enabled:    true,
		client:     c,
	}
	return proxy.Save()
}

func (c *Client) Proxy(name string) (*Proxy, error) {
	req, err := http.NewRequest("GET", c.endpoint+"/proxies/"+name, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to make proxy request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to http get proxy: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error getting proxy %s: %s %s", name, resp.Status, body)
	}

	var p Proxy
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return nil, fmt.Errorf("error decoding json for proxy %s: %w", name, err)
	}
	p.client = c

	return &p, nil
}

func (c *Client) ResetState() error {
	req, err := http.NewRequest("POST", c.endpoint+"/reset", http.NoBody)
	if err != nil {
		return fmt.Errorf("failed to make reset request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to http post reset: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error resetting proxies: %s %s", resp.Status, body)
	}

	return nil
}
