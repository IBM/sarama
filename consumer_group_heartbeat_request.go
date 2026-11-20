package sarama

// ConsumerGroupHeartbeatRequest sends a consumer group heartbeat using the KIP-848 protocol.
type ConsumerGroupHeartbeatRequest struct {
	Version              int16
	GroupId              string
	MemberId             string
	MemberEpoch          int32
	InstanceId           *string
	RackId               *string
	RebalanceTimeoutMs   int32
	SubscribedTopicNames []string
	SubscribedTopicRegex *string
	ServerAssignor       *string
	TopicPartitions      []ConsumerGroupHeartbeatTopicPartitions
}

func NewConsumerGroupHeartbeatRequest(version KafkaVersion) *ConsumerGroupHeartbeatRequest {
	req := &ConsumerGroupHeartbeatRequest{
		RebalanceTimeoutMs: -1,
	}

	switch {
	case version.IsAtLeast(V4_0_0_0):
		req.Version = 1
	default:
		req.Version = 0
	}

	return req
}

func (r *ConsumerGroupHeartbeatRequest) encode(pe packetEncoder) error {
	if !r.isValidVersion() {
		return PacketEncodingError{"invalid or unsupported ConsumerGroupHeartbeatRequest version"}
	}

	if err := pe.putString(r.GroupId); err != nil {
		return err
	}

	if err := pe.putString(r.MemberId); err != nil {
		return err
	}

	pe.putInt32(r.MemberEpoch)

	if err := pe.putNullableString(r.InstanceId); err != nil {
		return err
	}

	if err := pe.putNullableString(r.RackId); err != nil {
		return err
	}

	pe.putInt32(r.RebalanceTimeoutMs)

	if r.SubscribedTopicNames == nil {
		pe.putUVarint(0)
	} else {
		pe.putUVarint(uint64(len(r.SubscribedTopicNames)) + 1)
		for _, name := range r.SubscribedTopicNames {
			if err := pe.putString(name); err != nil {
				return err
			}
		}
	}

	if r.Version >= 1 {
		if err := pe.putNullableString(r.SubscribedTopicRegex); err != nil {
			return err
		}
	}

	if err := pe.putNullableString(r.ServerAssignor); err != nil {
		return err
	}

	if r.TopicPartitions == nil {
		pe.putUVarint(0)
	} else {
		pe.putUVarint(uint64(len(r.TopicPartitions)) + 1)
		for i := range r.TopicPartitions {
			if err := r.TopicPartitions[i].encode(pe); err != nil {
				return err
			}
		}
	}

	pe.putEmptyTaggedFieldArray()

	return nil
}

func (r *ConsumerGroupHeartbeatRequest) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version

	if !r.isValidVersion() {
		return PacketDecodingError{"invalid or unsupported ConsumerGroupHeartbeatRequest version"}
	}

	if r.GroupId, err = pd.getString(); err != nil {
		return err
	}

	if r.MemberId, err = pd.getString(); err != nil {
		return err
	}

	if r.MemberEpoch, err = pd.getInt32(); err != nil {
		return err
	}

	if r.InstanceId, err = pd.getNullableString(); err != nil {
		return err
	}

	if r.RackId, err = pd.getNullableString(); err != nil {
		return err
	}

	if r.RebalanceTimeoutMs, err = pd.getInt32(); err != nil {
		return err
	}

	topicCount, err := pd.getUVarint()
	if err != nil {
		return err
	}
	if topicCount == 0 {
		r.SubscribedTopicNames = nil
	} else {
		n := int(topicCount - 1)
		r.SubscribedTopicNames = make([]string, n)
		for i := range r.SubscribedTopicNames {
			if r.SubscribedTopicNames[i], err = pd.getString(); err != nil {
				return err
			}
		}
	}

	if r.Version >= 1 {
		if r.SubscribedTopicRegex, err = pd.getNullableString(); err != nil {
			return err
		}
	}

	if r.ServerAssignor, err = pd.getNullableString(); err != nil {
		return err
	}

	tpCount, err := pd.getUVarint()
	if err != nil {
		return err
	}
	if tpCount == 0 {
		r.TopicPartitions = nil
	} else {
		n := int(tpCount - 1)
		r.TopicPartitions = make([]ConsumerGroupHeartbeatTopicPartitions, n)
		for i := range r.TopicPartitions {
			if err := r.TopicPartitions[i].decode(pd, version); err != nil {
				return err
			}
		}
	}

	_, err = pd.getEmptyTaggedFieldArray()

	return err
}

func (r *ConsumerGroupHeartbeatRequest) key() int16 { return apiKeyConsumerGroupHeartbeat }

func (r *ConsumerGroupHeartbeatRequest) version() int16 { return r.Version }

func (r *ConsumerGroupHeartbeatRequest) setVersion(v int16) { r.Version = v }

func (r *ConsumerGroupHeartbeatRequest) headerVersion() int16 { return 2 }

func (r *ConsumerGroupHeartbeatRequest) isValidVersion() bool { return r.Version >= 0 && r.Version <= 1 }

func (r *ConsumerGroupHeartbeatRequest) isFlexible() bool { return true }

func (r *ConsumerGroupHeartbeatRequest) isFlexibleVersion(version int16) bool { return version >= 0 }

func (r *ConsumerGroupHeartbeatRequest) requiredVersion() KafkaVersion {
	switch r.Version {
	case 1:
		return V4_0_0_0
	default:
		return V3_5_0_0
	}
}
