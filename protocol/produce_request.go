package protocol

type ProduceRequest struct {
	RequiredAcks int16
	Timeout      int32
	msgSets      map[string]map[int32]*MessageSet
}

func (p *ProduceRequest) encode(pe packetEncoder) {
	pe.putInt16(p.RequiredAcks)
	pe.putInt32(p.Timeout)
	pe.putArrayCount(len(p.msgSets))
	for topic, partitions := range p.msgSets {
		pe.putString(topic)
		pe.putArrayCount(len(partitions))
		for id, msgSet := range partitions {
			pe.putInt32(id)
			pe.pushLength32()
			msgSet.encode(pe)
			pe.pop()
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
		set = new(MessageSet)
		p.msgSets[topic][partition] = set
	}

	set.addMessage(msg)
}
