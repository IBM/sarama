package sarama

import "errors"

const (
	// ReceiveTime is a special value for the timestamp field of Offset Commit Requests which
	// tells the broker to set the timestamp to the time at which the request was received.
	// The timestamp is only used if message version 1 is used, which requires kafka 0.8.2.
	ReceiveTime int64 = -1

	// GroupGenerationUndefined is a special value for the group generation field of
	// Offset Commit Requests that should be used when a consumer group does not rely
	// on Kafka for partition management.
	GroupGenerationUndefined = -1
)

type offsetCommitRequestBlock struct {
	offset    int64
	timestamp int64
	metadata  string
}

func (o *offsetCommitRequestBlock) encode(pe packetEncoder, version int16) error {
	pe.putInt64(o.offset)
	if version == 1 {
		pe.putInt64(o.timestamp)
	} else if o.timestamp != 0 {
		Logger.Println("Non-zero timestamp specified for OffsetCommitRequest not v1, it will be ignored")
	}

	return pe.putString(o.metadata)
}

func (o *offsetCommitRequestBlock) decode(pd packetDecoder, version int16) (err error) {
	if o.offset, err = pd.getInt64(); err != nil {
		return err
	}
	if version == 1 {
		if o.timestamp, err = pd.getInt64(); err != nil {
			return err
		}
	}
	o.metadata, err = pd.getString()
	return err
}

//OffsetCommitRequest is an offset commit request
type OffsetCommitRequest struct {
	ConsumerGroup           string
	ConsumerGroupGeneration int32  // v1 or later
	ConsumerID              string // v1 or later
	RetentionTime           int64  // v2 or later

	// Version can be:
	// - 0 (kafka 0.8.1 and later)
	// - 1 (kafka 0.8.2 and later)
	// - 2 (kafka 0.9.0 and later)
	// - 3 (kafka 0.11.0 and later)
	// - 4 (kafka 2.0.0 and later)
	Version int16
	blocks  map[string]map[int32]*offsetCommitRequestBlock
}

func (o *OffsetCommitRequest) encode(pe packetEncoder) error {
	if o.Version < 0 || o.Version > 4 {
		return PacketEncodingError{"invalid or unsupported OffsetCommitRequest version field"}
	}

	if err := pe.putString(o.ConsumerGroup); err != nil {
		return err
	}

	if o.Version >= 1 {
		pe.putInt32(o.ConsumerGroupGeneration)
		if err := pe.putString(o.ConsumerID); err != nil {
			return err
		}
	} else {
		if o.ConsumerGroupGeneration != 0 {
			Logger.Println("Non-zero ConsumerGroupGeneration specified for OffsetCommitRequest v0, it will be ignored")
		}
		if o.ConsumerID != "" {
			Logger.Println("Non-empty ConsumerID specified for OffsetCommitRequest v0, it will be ignored")
		}
	}

	if o.Version >= 2 {
		pe.putInt64(o.RetentionTime)
	} else if o.RetentionTime != 0 {
		Logger.Println("Non-zero RetentionTime specified for OffsetCommitRequest version <2, it will be ignored")
	}

	if err := pe.putArrayLength(len(o.blocks)); err != nil {
		return err
	}
	for topic, partitions := range o.blocks {
		if err := pe.putString(topic); err != nil {
			return err
		}
		if err := pe.putArrayLength(len(partitions)); err != nil {
			return err
		}
		for partition, block := range partitions {
			pe.putInt32(partition)
			if err := block.encode(pe, o.Version); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *OffsetCommitRequest) decode(pd packetDecoder, version int16) (err error) {
	o.Version = version

	if o.ConsumerGroup, err = pd.getString(); err != nil {
		return err
	}

	if o.Version >= 1 {
		if o.ConsumerGroupGeneration, err = pd.getInt32(); err != nil {
			return err
		}
		if o.ConsumerID, err = pd.getString(); err != nil {
			return err
		}
	}

	if o.Version >= 2 {
		if o.RetentionTime, err = pd.getInt64(); err != nil {
			return err
		}
	}

	topicCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if topicCount == 0 {
		return nil
	}
	o.blocks = make(map[string]map[int32]*offsetCommitRequestBlock)
	for i := 0; i < topicCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		o.blocks[topic] = make(map[int32]*offsetCommitRequestBlock)
		for j := 0; j < partitionCount; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			block := &offsetCommitRequestBlock{}
			if err := block.decode(pd, o.Version); err != nil {
				return err
			}
			o.blocks[topic][partition] = block
		}
	}
	return nil
}

func (o *OffsetCommitRequest) key() int16 {
	return 8
}

func (o *OffsetCommitRequest) version() int16 {
	return o.Version
}

func (o *OffsetCommitRequest) requiredVersion() KafkaVersion {
	switch o.Version {
	case 1:
		return V0_8_2_0
	case 2:
		return V0_9_0_0
	case 3:
		return V0_11_0_0
	case 4:
		return V2_0_0_0
	default:
		return MinVersion
	}
}

//AddBlock adds a block to an offset commit request
func (o *OffsetCommitRequest) AddBlock(topic string, partitionID int32, offset int64, timestamp int64, metadata string) {
	if o.blocks == nil {
		o.blocks = make(map[string]map[int32]*offsetCommitRequestBlock)
	}

	if o.blocks[topic] == nil {
		o.blocks[topic] = make(map[int32]*offsetCommitRequestBlock)
	}

	o.blocks[topic][partitionID] = &offsetCommitRequestBlock{offset, timestamp, metadata}
}

//Offset returns offset and metadata for an offset commit request
func (o *OffsetCommitRequest) Offset(topic string, partitionID int32) (int64, string, error) {
	partitions := o.blocks[topic]
	if partitions == nil {
		return 0, "", errors.New("no such offset")
	}
	block := partitions[partitionID]
	if block == nil {
		return 0, "", errors.New("no such offset")
	}
	return block.offset, block.metadata, nil
}
