package sarama

type MetadataRequest struct {
	Topics []string

	// v1 - all topics = null array, no topics = empty array
	AllTopics bool

	Version int16 // v1 requires 0.10+
}

func NewMetadataRequest(v KafkaVersion, topics []string) *MetadataRequest {
	switch {
	case v.IsAtLeast(V0_10_0_0):
		return &MetadataRequest{Topics: topics, Version: 1}
	default:
		return &MetadataRequest{Topics: topics}
	}

}

func (r *MetadataRequest) encode(pe packetEncoder) error {
	if r.Version == 1 {
		if r.AllTopics {
			// v1 - all topics = null array, no topics = empty array
			if err := pe.putArrayLength(-1); err != nil {
				return err
			}
			return nil
		}
	}

	err := pe.putArrayLength(len(r.Topics))
	if err != nil {
		return err
	}

	for i := range r.Topics {
		err = pe.putString(r.Topics[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *MetadataRequest) decode(pd packetDecoder, version int16) error {
	r.Version = version

	topicCount, err := pd.getNullableArrayLength()
	if err != nil {
		return err
	}
	if version == 1 {
		if topicCount == -1 {
			r.AllTopics = true
			return nil
		}
	}

	if topicCount <= 0 {
		return nil
	}

	r.Topics = make([]string, topicCount)
	for i := range r.Topics {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		r.Topics[i] = topic
	}
	return nil
}

func (r *MetadataRequest) key() int16 {
	return 3
}

func (r *MetadataRequest) version() int16 {
	return r.Version
}

func (r *MetadataRequest) requiredVersion() KafkaVersion {
	switch r.Version {
	case 1:
		return V0_10_0_0
	default:
		return minVersion
	}
}
