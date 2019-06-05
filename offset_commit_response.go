package sarama

//OffsetCommitResponse is an offset commit response type
type OffsetCommitResponse struct {
	Version        int16
	ThrottleTimeMs int32
	Errors         map[string]map[int32]KError
}

//AddError adds error to a commit response
func (o *OffsetCommitResponse) AddError(topic string, partition int32, kerror KError) {
	if o.Errors == nil {
		o.Errors = make(map[string]map[int32]KError)
	}
	partitions := o.Errors[topic]
	if partitions == nil {
		partitions = make(map[int32]KError)
		o.Errors[topic] = partitions
	}
	partitions[partition] = kerror
}

func (o *OffsetCommitResponse) encode(pe packetEncoder) error {
	if o.Version >= 3 {
		pe.putInt32(o.ThrottleTimeMs)
	}
	if err := pe.putArrayLength(len(o.Errors)); err != nil {
		return err
	}
	for topic, partitions := range o.Errors {
		if err := pe.putString(topic); err != nil {
			return err
		}
		if err := pe.putArrayLength(len(partitions)); err != nil {
			return err
		}
		for partition, kerror := range partitions {
			pe.putInt32(partition)
			pe.putInt16(int16(kerror))
		}
	}
	return nil
}

func (o *OffsetCommitResponse) decode(pd packetDecoder, version int16) (err error) {
	o.Version = version

	if version >= 3 {
		o.ThrottleTimeMs, err = pd.getInt32()
		if err != nil {
			return err
		}
	}

	numTopics, err := pd.getArrayLength()
	if err != nil || numTopics == 0 {
		return err
	}

	o.Errors = make(map[string]map[int32]KError, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.getString()
		if err != nil {
			return err
		}

		numErrors, err := pd.getArrayLength()
		if err != nil {
			return err
		}

		o.Errors[name] = make(map[int32]KError, numErrors)

		for j := 0; j < numErrors; j++ {
			id, err := pd.getInt32()
			if err != nil {
				return err
			}

			tmp, err := pd.getInt16()
			if err != nil {
				return err
			}
			o.Errors[name][id] = KError(tmp)
		}
	}

	return nil
}

func (o *OffsetCommitResponse) key() int16 {
	return 8
}

func (o *OffsetCommitResponse) version() int16 {
	return o.Version
}

func (o *OffsetCommitResponse) requiredVersion() KafkaVersion {
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
