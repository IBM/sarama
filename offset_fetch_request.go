package sarama

//OffsetFetchRequest is an offset fetch request type
type OffsetFetchRequest struct {
	Version       int16
	ConsumerGroup string
	partitions    map[string][]int32
}

func (o *OffsetFetchRequest) encode(pe packetEncoder) (err error) {
	if o.Version < 0 || o.Version > 5 {
		return PacketEncodingError{"invalid or unsupported OffsetFetchRequest version field"}
	}

	if err = pe.putString(o.ConsumerGroup); err != nil {
		return err
	}

	if o.Version >= 2 && o.partitions == nil {
		pe.putInt32(-1)
	} else {
		if err = pe.putArrayLength(len(o.partitions)); err != nil {
			return err
		}
		for topic, partitions := range o.partitions {
			if err = pe.putString(topic); err != nil {
				return err
			}
			if err = pe.putInt32Array(partitions); err != nil {
				return err
			}
		}
	}
	return nil
}

func (o *OffsetFetchRequest) decode(pd packetDecoder, version int16) (err error) {
	o.Version = version
	if o.ConsumerGroup, err = pd.getString(); err != nil {
		return err
	}
	partitionCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if (partitionCount == 0 && version < 2) || partitionCount < 0 {
		return nil
	}
	o.partitions = make(map[string][]int32)
	for i := 0; i < partitionCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitions, err := pd.getInt32Array()
		if err != nil {
			return err
		}
		o.partitions[topic] = partitions
	}
	return nil
}

func (o *OffsetFetchRequest) key() int16 {
	return 9
}

func (o *OffsetFetchRequest) version() int16 {
	return o.Version
}

func (o *OffsetFetchRequest) requiredVersion() KafkaVersion {
	switch o.Version {
	case 1:
		return V0_8_2_0
	case 2:
		return V0_10_2_0
	case 3:
		return V0_11_0_0
	case 4:
		return V2_0_0_0
	case 5:
		return V2_1_0_0
	default:
		return MinVersion
	}
}

//ZeroPartitions sets the partitions to zero in a fetch request
func (o *OffsetFetchRequest) ZeroPartitions() {
	if o.partitions == nil && o.Version >= 2 {
		o.partitions = make(map[string][]int32)
	}
}

//AddPartition adds partitions to a fetch request
func (o *OffsetFetchRequest) AddPartition(topic string, partitionID int32) {
	if o.partitions == nil {
		o.partitions = make(map[string][]int32)
	}

	o.partitions[topic] = append(o.partitions[topic], partitionID)
}
