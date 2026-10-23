package sarama

type DescribeQuorumRequest struct {
	Version int16

	// Topics is the set of topics (and their partitions) to describe the
	// quorum of. In practice this is the __cluster_metadata topic.
	Topics []DescribeQuorumRequestTopic
}

func NewDescribeQuorumRequest(version KafkaVersion) *DescribeQuorumRequest {
	req := &DescribeQuorumRequest{}

	switch {
	case version.IsAtLeast(V3_9_0_0):
		req.Version = 2
	case version.IsAtLeast(V3_3_0_0):
		req.Version = 1
	default:
		req.Version = 0
	}

	return req
}

func (r *DescribeQuorumRequest) setVersion(v int16) {
	r.Version = v
}

type DescribeQuorumRequestTopic struct {
	// TopicName is the topic name
	TopicName string

	// PartitionIndexes is the indexes of the partitions to describe
	PartitionIndexes []int32
}

func (t *DescribeQuorumRequestTopic) encode(pe packetEncoder) error {
	if err := pe.putString(t.TopicName); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(t.PartitionIndexes)); err != nil {
		return err
	}
	for _, partition := range t.PartitionIndexes {
		pe.putInt32(partition)
		pe.putEmptyTaggedFieldArray()
	}

	pe.putEmptyTaggedFieldArray()
	return nil
}

func (t *DescribeQuorumRequestTopic) decode(pd packetDecoder, version int16) (err error) {
	if t.TopicName, err = pd.getString(); err != nil {
		return err
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if n < 0 {
		return errInvalidArrayLength
	}

	t.PartitionIndexes = make([]int32, n)
	for i := range n {
		if t.PartitionIndexes[i], err = pd.getInt32(); err != nil {
			return err
		}
		if _, err = pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}

	_, err = pd.getEmptyTaggedFieldArray()
	return err
}

func (r *DescribeQuorumRequest) encode(pe packetEncoder) error {
	if err := pe.putArrayLength(len(r.Topics)); err != nil {
		return err
	}
	for i := range r.Topics {
		if err := r.Topics[i].encode(pe); err != nil {
			return err
		}
	}

	pe.putEmptyTaggedFieldArray()
	return nil
}

func (r *DescribeQuorumRequest) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if n < 0 {
		return errInvalidArrayLength
	}

	r.Topics = make([]DescribeQuorumRequestTopic, n)
	for i := range n {
		if err := r.Topics[i].decode(pd, version); err != nil {
			return err
		}
	}

	_, err = pd.getEmptyTaggedFieldArray()
	return err
}

func (r *DescribeQuorumRequest) key() int16 {
	return apiKeyDescribeQuorum
}

func (r *DescribeQuorumRequest) version() int16 {
	return r.Version
}

func (r *DescribeQuorumRequest) headerVersion() int16 {
	return 2
}

func (r *DescribeQuorumRequest) isValidVersion() bool {
	return r.Version >= 0 && r.Version <= 2
}

func (r *DescribeQuorumRequest) isFlexible() bool {
	return r.isFlexibleVersion(r.Version)
}

func (r *DescribeQuorumRequest) isFlexibleVersion(version int16) bool {
	return version >= 0
}

func (r *DescribeQuorumRequest) requiredVersion() KafkaVersion {
	switch r.Version {
	case 2:
		return V3_9_0_0
	case 1:
		return V3_3_0_0
	default:
		return V2_7_0_0
	}
}
