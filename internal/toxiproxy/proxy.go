package toxiproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Proxy struct {
	client     *Client
	Name       string `json:"name"`
	ListenAddr string `json:"listen"`
	TargetAddr string `json:"upstream"`
	Enabled    bool   `json:"enabled"`
}

type Attributes map[string]int

func (p *Proxy) AddToxic(
	name string,
	toxicType string,
	stream string,
	toxicity float32,
	attributes Attributes,
) (*Toxic, error) {
	toxic := &Toxic{
		Name:       name,
		Type:       toxicType,
		Stream:     stream,
		Toxicity:   toxicity,
		Attributes: attributes,
	}
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(&toxic); err != nil {
		return nil, fmt.Errorf("failed to json encode toxic: %w", err)
	}
	body := bytes.NewReader(b.Bytes())

	c := p.client
	req, err := http.NewRequest("POST", c.endpoint+"/proxies/"+p.Name+"/toxics", body)
	if err != nil {
		return nil, fmt.Errorf("failed to make post toxic request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to http post toxic: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error creating toxic %s: %s %s", name, resp.Status, body)
	}

	return toxic, nil
}

func (p *Proxy) Enable() error {
	p.Enabled = true
	_, err := p.Save()
	return err
}

func (p *Proxy) Disable() error {
	p.Enabled = false
	_, err := p.Save()
	return err
}

func (p *Proxy) Save() (*Proxy, error) {
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(&p); err != nil {
		return nil, fmt.Errorf("failed to json encode proxy: %w", err)
	}
	body := bytes.NewReader(b.Bytes())

	c := p.client
	req, err := http.NewRequest("POST", c.endpoint+"/proxies/"+p.Name, body)
	if err != nil {
		return nil, fmt.Errorf("failed to make post proxy request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to http post proxy: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		if _, err := body.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("failed to rewind post body: %w", err)
		}
		req, err = http.NewRequest("POST", c.endpoint+"/proxies", body)
		if err != nil {
			return nil, fmt.Errorf("failed to make post proxy request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err = c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to http post proxy: %w", err)
		}
		defer resp.Body.Close()
	}

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error saving proxy: %s %s", resp.Status, body)
	}

	return p, nil
}
