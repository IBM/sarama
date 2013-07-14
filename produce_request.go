package kafka

const (
	NO_RESPONSE    int16 = 0
	WAIT_FOR_LOCAL int16 = 1
	WAIT_FOR_ALL   int16 = -1
)

type ProduceRequest struct {
	ResponseCondition int16
	Timeout           int32
	MsgSets           map[*string]map[int32]*messageSet
}

func (p *ProduceRequest) encode(pe packetEncoder) {
	pe.putInt16(p.ResponseCondition)
	pe.putInt32(p.Timeout)
	pe.putArrayCount(len(p.MsgSets))
	for topic, partitions := range p.MsgSets {
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

func (p *ProduceRequest) AddMessageSet(topic *string, partition int32, set *messageSet) {
	if p.MsgSets == nil {
		p.MsgSets = make(map[*string]map[int32]*messageSet)
	}

	if p.MsgSets[topic] == nil {
		p.MsgSets[topic] = make(map[int32]*messageSet)
	}

	p.MsgSets[topic][partition] = set
}
