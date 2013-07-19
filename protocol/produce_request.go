package protocol

import enc "sarama/encoding"
import "sarama/types"

type ProduceRequest struct {
	RequiredAcks types.RequiredAcks
	Timeout      int32
	msgSets      map[string]map[int32]*MessageSet
}

func (p *ProduceRequest) Encode(pe enc.PacketEncoder) error {
	pe.PutInt16(p.RequiredAcks)
	pe.PutInt32(p.Timeout)
	err := pe.PutArrayLength(len(p.msgSets))
	if err != nil {
		return err
	}
	for topic, partitions := range p.msgSets {
		err = pe.PutString(topic)
		if err != nil {
			return err
		}
		err = pe.PutArrayLength(len(partitions))
		if err != nil {
			return err
		}
		for id, msgSet := range partitions {
			pe.PutInt32(id)
			pe.PushLength32()
			err = msgSet.Encode(pe)
			if err != nil {
				return err
			}
			err = pe.Pop()
			if err != nil {
				return err
			}
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
