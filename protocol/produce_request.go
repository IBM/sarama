package protocol

// Special values accepted by Kafka for the ResponseCondition member of produce requests.
const (
	NO_RESPONSE    int16 = 0  // Don't send any response, the TCP ACK is all you get.
	WAIT_FOR_LOCAL int16 = 1  // Wait for only the local commit to succeed before responding.
	WAIT_FOR_ALL   int16 = -1 // Wait for all replicas to commit before responding.
)

type ProduceRequest struct {
	ResponseCondition int16
	Timeout           int32
	msgSets           map[string]map[int32]*MessageSet
}

func (p *ProduceRequest) encode(pe packetEncoder) {
	pe.putInt16(p.ResponseCondition)
	pe.putInt32(p.Timeout)
	pe.putArrayCount(len(p.msgSets))
	for topic, partitions := range p.msgSets {
		pe.putString(topic)
		pe.putArrayCount(len(partitions))
		for id, msgSet := range partitions {
			pe.putInt32(id)
			msgSet.encode(pe)
		}
	}
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
		set = newMessageSet()
		p.msgSets[topic][partition] = set
	}

	set.addMessage(msg)
}
