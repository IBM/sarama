package toxiproxy

type Proxy struct {
	Enabled bool
}

type Attributes map[string]int

func (p *Proxy) AddToxic(name string, toxicType string, stream string, toxicity float32, attributes Attributes) (*Toxic, error) {
	return &Toxic{}, nil
}

func (p *Proxy) Enable() error {
	return nil
}

func (p *Proxy) Disable() error {
	return nil
}

func (p *Proxy) Save() error {
	return nil
}
