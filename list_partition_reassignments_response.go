package sarama

type listPartitionReassignmentsResponseBlock struct {
	replicas         []int32
	addingReplicas   []int32
	removingReplicas []int32
}

func (b *listPartitionReassignmentsResponseBlock) encode(pe packetEncoder) error {

	if err := pe.putCompactInt32Array(b.replicas); err != nil {
		return err
	}
	if err := pe.putCompactInt32Array(b.addingReplicas); err != nil {
		return err
	}
	if err := pe.putCompactInt32Array(b.removingReplicas); err != nil {
		return err
	}

	pe.putEmptyTaggedFieldArray()

	return nil
}

func (b *listPartitionReassignmentsResponseBlock) decode(pd packetDecoder) (err error) {

	if b.replicas, err = pd.getCompactInt32Array(); err != nil {
		return err
	}

	if b.addingReplicas, err = pd.getCompactInt32Array(); err != nil {
		return err
	}

	if b.removingReplicas, err = pd.getCompactInt32Array(); err != nil {
		return err
	}

	if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
		return err
	}

	return err
}

type ListPartitionReassignmentsResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      KError
	ErrorMessage   *string
	blocks         map[string]map[int32]*listPartitionReassignmentsResponseBlock
}

func (r *ListPartitionReassignmentsResponse) AddBlock(topic string, partition int32, replicas, addingReplicas, removingReplicas []int32) {
	if r.blocks == nil {
		r.blocks = make(map[string]map[int32]*listPartitionReassignmentsResponseBlock)
	}
	partitions := r.blocks[topic]
	if partitions == nil {
		partitions = make(map[int32]*listPartitionReassignmentsResponseBlock)
		r.blocks[topic] = partitions
	}

	partitions[partition] = &listPartitionReassignmentsResponseBlock{replicas: replicas, addingReplicas: addingReplicas, removingReplicas: removingReplicas}
}

func (r *ListPartitionReassignmentsResponse) encode(pe packetEncoder) error {
	pe.putInt32(r.ThrottleTimeMs)
	pe.putInt16(int16(r.ErrorCode))
	if err := pe.putNullableCompactString(r.ErrorMessage); err != nil {
		return err
	}

	pe.putCompactArrayLength(len(r.blocks))
	for topic, partitions := range r.blocks {
		if err := pe.putCompactString(topic); err != nil {
			return err
		}
		pe.putCompactArrayLength(len(partitions))
		for partition, block := range partitions {
			pe.putInt32(partition)

			if err := block.encode(pe); err != nil {
				return err
			}
		}
		pe.putEmptyTaggedFieldArray()
	}

	pe.putEmptyTaggedFieldArray()

	return nil
}

func (r *ListPartitionReassignmentsResponse) decode(pd packetDecoder, version int16) (err error) {
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

	r.blocks = make(map[string]map[int32]*listPartitionReassignmentsResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		topic, err := pd.getCompactString()
		if err != nil {
			return err
		}

		ongoingPartitionReassignments, err := pd.getCompactArrayLength()
		if err != nil {
			return err
		}

		r.blocks[topic] = make(map[int32]*listPartitionReassignmentsResponseBlock, ongoingPartitionReassignments)

		for j := 0; j < ongoingPartitionReassignments; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}

			block := &listPartitionReassignmentsResponseBlock{}
			if err := block.decode(pd); err != nil {
				return err
			}
			r.blocks[topic][partition] = block
		}

		if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}
	if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
		return err
	}

	return nil
}

func (r *ListPartitionReassignmentsResponse) key() int16 {
	return 46
}

func (r *ListPartitionReassignmentsResponse) version() int16 {
	return r.Version
}

func (r *ListPartitionReassignmentsResponse) headerVersion() int16 {
	return 1
}

func (r *ListPartitionReassignmentsResponse) requiredVersion() KafkaVersion {
	return V2_4_0_0
}
