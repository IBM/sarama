package sarama

//type alterPartitionReassignmentsErrorBlock struct {
//	errorCode    KError
//	errorMessage string
//}
//
//func (b *alterPartitionReassignmentsErrorBlock) encode(pe packetEncoder) error {
//	pe.putInt32(b.errorCode)
//	pe.putString(b.errorMessage)
//
//	return nil
//}
//
//func (b *alterPartitionReassignmentsErrorBlock) decode(pd packetDecoder) (err error) {
//	if b.errorCode, err = pd.getInt32(); err != nil {
//		return err
//	}
//	b.errorMessage, err = pd.getString()
//	return err
//}

type AlterPartitionReassignmentsResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      KError
	ErrorMessage   *string
	//Errors         map[string]map[int32]*alterPartitionReassignmentsErrorBlock
}

//func (r *AlterPartitionReassignmentsResponse) AddError(topic string, partition int32, kerror KError) {
//	// TODO
//	if r.Errors == nil {
//		r.Errors = make(map[string]map[int32]KError)
//	}
//	partitions := r.Errors[topic]
//	if partitions == nil {
//		partitions = make(map[int32]KError)
//		r.Errors[topic] = partitions
//	}
//	partitions[partition] = kerror
//}

func (r *AlterPartitionReassignmentsResponse) encode(pe packetEncoder) error {
	pe.putInt32(r.ThrottleTimeMs)
	//pe.putInt16(int16(r.ErrorCode))
	//if err := pe.putNullableString(r.ErrorMessage); err != nil {
	//	return err
	//}

	//if err := pe.putArrayLength(len(r.Errors)); err != nil {
	//	return err
	//}
	//for topic, partitions := range r.Errors {
	//	if err := pe.putString(topic); err != nil {
	//		return err
	//	}
	//	if err := pe.putArrayLength(len(partitions)); err != nil {
	//		return err
	//	}
	//	for partition, kerror := range partitions {
	//		pe.putInt32(partition)
	//		pe.putInt16(int16(kerror))
	//	}
	//}
	return nil
}

func (r *AlterPartitionReassignmentsResponse) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version

	if r.ThrottleTimeMs, err = pd.getInt32(); err != nil {
		return err
	}

	//kerr, err := pd.getInt16();
	//if err != nil {
	//		return err
	//}
	//
	//r.ErrorCode = KError(kerr)
	//
	//if r.ErrorMessage, err = pd.getNullableString(); err != nil {
	//	return err
	//}

	return nil

	//numTopics, err := pd.getArrayLength()
	//if err != nil || numTopics == 0 {
	//	return err
	//}
	//
	//r.Errors = make(map[string]map[int32]KError, numTopics)
	//for i := 0; i < numTopics; i++ {
	//	name, err := pd.getString()
	//	if err != nil {
	//		return err
	//	}
	//
	//	numErrors, err := pd.getArrayLength()
	//	if err != nil {
	//		return err
	//	}
	//
	//	r.Errors[name] = make(map[int32]KError, numErrors)
	//
	//	for j := 0; j < numErrors; j++ {
	//		id, err := pd.getInt32()
	//		if err != nil {
	//			return err
	//		}
	//
	//		tmp, err := pd.getInt16()
	//		if err != nil {
	//			return err
	//		}
	//		r.Errors[name][id] = KError(tmp)
	//	}
	//}

	return nil
}

func (r *AlterPartitionReassignmentsResponse) key() int16 {
	return 45
}

func (r *AlterPartitionReassignmentsResponse) version() int16 {
	return r.Version
}

func (r *AlterPartitionReassignmentsResponse) headerVersion() int16 {
	return 1
}

func (r *AlterPartitionReassignmentsResponse) requiredVersion() KafkaVersion {
	return MinVersion
}
