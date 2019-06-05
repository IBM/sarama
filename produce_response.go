package sarama

import (
	"fmt"
	"time"
)

//ProduceResponseBlock is a produce response block
type ProduceResponseBlock struct {
	Err    KError
	Offset int64
	// only provided if Version >= 2 and the broker is configured with `LogAppendTime`
	Timestamp time.Time
}

func (p *ProduceResponseBlock) decode(pd packetDecoder, version int16) (err error) {
	tmp, err := pd.getInt16()
	if err != nil {
		return err
	}
	p.Err = KError(tmp)

	p.Offset, err = pd.getInt64()
	if err != nil {
		return err
	}

	if version >= 2 {
		if millis, err := pd.getInt64(); err != nil {
			return err
		} else if millis != -1 {
			p.Timestamp = time.Unix(millis/1000, (millis%1000)*int64(time.Millisecond))
		}
	}

	return nil
}

func (p *ProduceResponseBlock) encode(pe packetEncoder, version int16) (err error) {
	pe.putInt16(int16(p.Err))
	pe.putInt64(p.Offset)

	if version >= 2 {
		timestamp := int64(-1)
		if !p.Timestamp.Before(time.Unix(0, 0)) {
			timestamp = p.Timestamp.UnixNano() / int64(time.Millisecond)
		} else if !p.Timestamp.IsZero() {
			return PacketEncodingError{fmt.Sprintf("invalid timestamp (%v)", p.Timestamp)}
		}
		pe.putInt64(timestamp)
	}

	return nil
}

//ProduceResponse is a produce response type
type ProduceResponse struct {
	Blocks       map[string]map[int32]*ProduceResponseBlock
	Version      int16
	ThrottleTime time.Duration // only provided if Version >= 1
}

func (p *ProduceResponse) decode(pd packetDecoder, version int16) (err error) {
	p.Version = version

	numTopics, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	p.Blocks = make(map[string]map[int32]*ProduceResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.getString()
		if err != nil {
			return err
		}

		numBlocks, err := pd.getArrayLength()
		if err != nil {
			return err
		}

		p.Blocks[name] = make(map[int32]*ProduceResponseBlock, numBlocks)

		for j := 0; j < numBlocks; j++ {
			id, err := pd.getInt32()
			if err != nil {
				return err
			}

			block := new(ProduceResponseBlock)
			err = block.decode(pd, version)
			if err != nil {
				return err
			}
			p.Blocks[name][id] = block
		}
	}

	if p.Version >= 1 {
		millis, err := pd.getInt32()
		if err != nil {
			return err
		}

		p.ThrottleTime = time.Duration(millis) * time.Millisecond
	}

	return nil
}

func (p *ProduceResponse) encode(pe packetEncoder) error {
	err := pe.putArrayLength(len(p.Blocks))
	if err != nil {
		return err
	}
	for topic, partitions := range p.Blocks {
		err = pe.putString(topic)
		if err != nil {
			return err
		}
		err = pe.putArrayLength(len(partitions))
		if err != nil {
			return err
		}
		for id, prb := range partitions {
			pe.putInt32(id)
			err = prb.encode(pe, p.Version)
			if err != nil {
				return err
			}
		}
	}
	if p.Version >= 1 {
		pe.putInt32(int32(p.ThrottleTime / time.Millisecond))
	}
	return nil
}

func (p *ProduceResponse) key() int16 {
	return 0
}

func (p *ProduceResponse) version() int16 {
	return p.Version
}

func (p *ProduceResponse) requiredVersion() KafkaVersion {
	switch p.Version {
	case 1:
		return V0_9_0_0
	case 2:
		return V0_10_0_0
	case 3:
		return V0_11_0_0
	default:
		return MinVersion
	}
}

//GetBlock return a produce response block
func (p *ProduceResponse) GetBlock(topic string, partition int32) *ProduceResponseBlock {
	if p.Blocks == nil {
		return nil
	}

	if p.Blocks[topic] == nil {
		return nil
	}

	return p.Blocks[topic][partition]
}

// Testing API

//AddTopicPartition adds a given topic and partition
func (p *ProduceResponse) AddTopicPartition(topic string, partition int32, err KError) {
	if p.Blocks == nil {
		p.Blocks = make(map[string]map[int32]*ProduceResponseBlock)
	}
	byTopic, ok := p.Blocks[topic]
	if !ok {
		byTopic = make(map[int32]*ProduceResponseBlock)
		p.Blocks[topic] = byTopic
	}
	block := &ProduceResponseBlock{
		Err: err,
	}
	if p.Version >= 2 {
		block.Timestamp = time.Now()
	}
	byTopic[partition] = block
}
