package toxiproxy

type Toxic struct {
	Name       string     `json:"name"`
	Type       string     `json:"type"`
	Stream     string     `json:"stream,omitempty"`
	Toxicity   float32    `json:"toxicity"`
	Attributes Attributes `json:"attributes"`
}
