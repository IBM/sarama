package sarama

// RequiredAcks is used in Produce Requests to tell the broker how many replica acknowledgements
// it must see before responding. Any positive int16 value is valid, or the constants defined here.
type RequiredAcks int16

const (
	// NoResponse doesn't send any response, the TCP ACK is all you get.
	NoResponse RequiredAcks = 0
	// WaitForLocal waits for only the local commit to succeed before responding.
	WaitForLocal RequiredAcks = 1
	// WaitForAll waits for all replicas to commit before responding.
	WaitForAll RequiredAcks = -1
)

type ProduceRequest struct {
	RequiredAcks RequiredAcks
	Timeout      int32
	msgSets      map[string]map[int32]*MessageSet
}

func (p *ProduceRequest) encode(pe packetEncoder) error {
	pe.putInt16(int16(p.RequiredAcks))
	pe.putInt32(p.Timeout)
	err := pe.putArrayLength(len(p.msgSets))
	if err != nil {
		return err
	}
	for topic, partitions := range p.msgSets {
		err = pe.putString(topic)
		if err != nil {
			return err
		}
		err = pe.putArrayLength(len(partitions))
		if err != nil {
			return err
		}
		for id, msgSet := range partitions {
			pe.putInt32(id)
			pe.push(&lengthField{})
			err = msgSet.encode(pe)
			if err != nil {
				return err
			}
			err = pe.pop()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *ProduceRequest) key() int16 {
	return 0
}

func (p *ProduceRequest) version() int16 {
	return 0
}

func (p *ProduceRequest) AddMessage(topic string, partition int32, msg *Message) {
	if p.msgSets == nil {
		p.msgSets = make(map[string]map[int32]*MessageSet)
	}

	if p.msgSets[topic] == nil {
		p.msgSets[topic] = make(map[int32]*MessageSet)
	}

	set := p.msgSets[topic][partition]

	if set == nil {
		set = new(MessageSet)
		p.msgSets[topic][partition] = set
	}

	set.addMessage(msg)
}
