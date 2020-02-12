package sarama

type alterPartitionReassignmentsErrorBlock struct {
	errorCode    KError
	errorMessage string
}

func (b *alterPartitionReassignmentsErrorBlock) encode(pe packetEncoder) error {
	pe.putInt32(int32(b.errorCode))
	pe.putString(b.errorMessage)

	return nil
}

func (b *alterPartitionReassignmentsErrorBlock) decode(pd packetDecoder) (err error) {
	errorCode, err := pd.getInt32()
	if err != nil {
		return err
	}
	b.errorCode = KError(errorCode)
	b.errorMessage, err = pd.getString()
	return err
}

type AlterPartitionReassignmentsResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      KError
	ErrorMessage   *string
	Errors         map[string]map[int32]*alterPartitionReassignmentsErrorBlock
}

func (r *AlterPartitionReassignmentsResponse) AddError(topic string, partition int32, kerror KError, message *string) {
	if r.Errors == nil {
		r.Errors = make(map[string]map[int32]*alterPartitionReassignmentsErrorBlock)
	}
	partitions := r.Errors[topic]
	if partitions == nil {
		partitions = make(map[int32]*alterPartitionReassignmentsErrorBlock)
		r.Errors[topic] = partitions
	}

	partitions[partition] = &alterPartitionReassignmentsErrorBlock{errorCode: kerror, errorMessage: *message}
}

func (r *AlterPartitionReassignmentsResponse) encode(pe packetEncoder) error {
	pe.putInt32(r.ThrottleTimeMs)
	pe.putInt16(int16(r.ErrorCode))
	if err := pe.putNullableString(r.ErrorMessage); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(r.Errors)); err != nil {
		return err
	}
	for topic, partitions := range r.Errors {
		if err := pe.putString(topic); err != nil {
			return err
		}
		if err := pe.putArrayLength(len(partitions)); err != nil {
			return err
		}
		for partition, kerror := range partitions {
			pe.putInt32(partition)
			pe.putInt16(int16(kerror.errorCode))
		}
	}
	return nil
}

func (r *AlterPartitionReassignmentsResponse) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version

	if r.ThrottleTimeMs, err = pd.getInt32(); err != nil {
		return err
	}

	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}

	r.ErrorCode = KError(kerr)

	if r.ErrorMessage, err = pd.getCompactNullableString(); err != nil {
		return err
	}

	numTopics, err := pd.getCompactArrayLength()
	if err != nil || numTopics == 0 {
		return err
	}

	r.Errors = make(map[string]map[int32]*alterPartitionReassignmentsErrorBlock, numTopics)
	for i := 0; i < int(numTopics); i++ {
		name, err := pd.getCompactString()
		if err != nil {
			return err
		}

		ongoingPartitionReassignments, err := pd.getCompactArrayLength()
		if err != nil {
			return err
		}

		r.Errors[name] = make(map[int32]*alterPartitionReassignmentsErrorBlock, ongoingPartitionReassignments)

		for j := 0; j < ongoingPartitionReassignments; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			errorCode, err := pd.getInt16()
			if err != nil {
				return err
			}
			errorMessage, err := pd.getCompactNullableString()
			if err != nil {
				return err
			}

			if errorCode != 0 {
				if errorMessage == nil {
					errorMessage = new(string)
				}

				r.AddError(name, partition, KError(errorCode), errorMessage)
			}

			// Tag buffer?
			pd.getInt8()
		}
		// Tag buffer?
		pd.getInt8()
	}
	// Tag buffer?
	pd.getInt8()

	return nil
}

func (r *AlterPartitionReassignmentsResponse) key() int16 {
	return 45
}

func (r *AlterPartitionReassignmentsResponse) version() int16 {
	return r.Version
}

func (r *AlterPartitionReassignmentsResponse) headerVersion() int16 {
	return 2
}

func (r *AlterPartitionReassignmentsResponse) requiredVersion() KafkaVersion {
	return V2_4_0_0
}
