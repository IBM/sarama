package sarama

import "time"

// ConsumerGroupHeartbeatTopicPartitions contains the partitions of a topic.
type ConsumerGroupHeartbeatTopicPartitions struct {
	TopicID    Uuid
	Partitions []int32
}

func (t *ConsumerGroupHeartbeatTopicPartitions) encode(pe packetEncoder) error {
	if err := pe.putUuid(t.TopicID); err != nil {
		return err
	}

	partitions := t.Partitions
	if partitions == nil {
		partitions = []int32{}
	}

	if err := pe.putInt32Array(partitions); err != nil {
		return err
	}

	pe.putEmptyTaggedFieldArray()

	return nil
}

func (t *ConsumerGroupHeartbeatTopicPartitions) decode(pd packetDecoder, version int16) (err error) {
	if t.TopicID, err = pd.getUuid(); err != nil {
		return err
	}

	if t.Partitions, err = pd.getInt32Array(); err != nil {
		return err
	}

	_, err = pd.getEmptyTaggedFieldArray()

	return err
}

// ConsumerGroupHeartbeatAssignment contains the partitions assigned to a consumer.
type ConsumerGroupHeartbeatAssignment struct {
	TopicPartitions []ConsumerGroupHeartbeatTopicPartitions
}

func (a *ConsumerGroupHeartbeatAssignment) encode(pe packetEncoder) error {
	if err := pe.putArrayLength(len(a.TopicPartitions)); err != nil {
		return err
	}

	for i := range a.TopicPartitions {
		if err := a.TopicPartitions[i].encode(pe); err != nil {
			return err
		}
	}

	pe.putEmptyTaggedFieldArray()

	return nil
}

func (a *ConsumerGroupHeartbeatAssignment) decode(pd packetDecoder, version int16) (err error) {
	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	if n < 0 {
		return errInvalidArrayLength
	}

	a.TopicPartitions = make([]ConsumerGroupHeartbeatTopicPartitions, n)

	for i := range a.TopicPartitions {
		if err := a.TopicPartitions[i].decode(pd, version); err != nil {
			return err
		}
	}

	_, err = pd.getEmptyTaggedFieldArray()

	return err
}

// ConsumerGroupHeartbeatResponse contains the coordinator response to a heartbeat.
type ConsumerGroupHeartbeatResponse struct {
	Version             int16
	ThrottleTime        time.Duration
	ErrorCode           KError
	ErrorMessage        *string
	MemberId            *string
	MemberEpoch         int32
	HeartbeatIntervalMs int32
	Assignment          *ConsumerGroupHeartbeatAssignment
}

func (r *ConsumerGroupHeartbeatResponse) encode(pe packetEncoder) error {
	if !r.isValidVersion() {
		return PacketEncodingError{"invalid or unsupported ConsumerGroupHeartbeatResponse version"}
	}

	pe.putDurationMs(r.ThrottleTime)
	pe.putKError(r.ErrorCode)

	if err := pe.putNullableString(r.ErrorMessage); err != nil {
		return err
	}

	if err := pe.putNullableString(r.MemberId); err != nil {
		return err
	}

	pe.putInt32(r.MemberEpoch)
	pe.putInt32(r.HeartbeatIntervalMs)

	if r.Assignment == nil {
		pe.putInt8(-1)
	} else {
		pe.putInt8(1)
		if err := r.Assignment.encode(pe); err != nil {
			return err
		}
	}

	pe.putEmptyTaggedFieldArray()

	return nil
}

func (r *ConsumerGroupHeartbeatResponse) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version

	if !r.isValidVersion() {
		return PacketDecodingError{"invalid or unsupported ConsumerGroupHeartbeatResponse version"}
	}

	if r.ThrottleTime, err = pd.getDurationMs(); err != nil {
		return err
	}

	if r.ErrorCode, err = pd.getKError(); err != nil {
		return err
	}

	if r.ErrorMessage, err = pd.getNullableString(); err != nil {
		return err
	}

	if r.MemberId, err = pd.getNullableString(); err != nil {
		return err
	}

	if r.MemberEpoch, err = pd.getInt32(); err != nil {
		return err
	}

	if r.HeartbeatIntervalMs, err = pd.getInt32(); err != nil {
		return err
	}

	hasAssignment, err := pd.getInt8()
	if err != nil {
		return err
	}

	if hasAssignment < 0 {
		r.Assignment = nil
	} else {
		r.Assignment = new(ConsumerGroupHeartbeatAssignment)
		if err := r.Assignment.decode(pd, version); err != nil {
			return err
		}
	}

	_, err = pd.getEmptyTaggedFieldArray()

	return err
}

func (r *ConsumerGroupHeartbeatResponse) key() int16 { return apiKeyConsumerGroupHeartbeat }

func (r *ConsumerGroupHeartbeatResponse) version() int16 { return r.Version }

func (r *ConsumerGroupHeartbeatResponse) setVersion(v int16) { r.Version = v }

func (r *ConsumerGroupHeartbeatResponse) headerVersion() int16 { return 1 }

func (r *ConsumerGroupHeartbeatResponse) isValidVersion() bool {
	return r.Version >= 0 && r.Version <= 1
}

func (r *ConsumerGroupHeartbeatResponse) isFlexible() bool { return true }

func (r *ConsumerGroupHeartbeatResponse) isFlexibleVersion(version int16) bool { return version >= 0 }

func (r *ConsumerGroupHeartbeatResponse) requiredVersion() KafkaVersion {
	switch r.Version {
	case 1:
		return V4_0_0_0
	default:
		return V3_5_0_0
	}
}

func (r *ConsumerGroupHeartbeatResponse) throttleTime() time.Duration { return r.ThrottleTime }
