package sarama

import "time"

type ListOffsetResponse struct {
	// Valid versions: 0 - 5
	//
	// Version 1 removes the offsets array in favor of returning a single offset.
	// Version 1 also adds the timestamp associated with the returned offset.
	//
	// Version 2 adds the throttle time.
	//
	// Starting in version 3, on quota violation, brokers send out responses before throttling.
	//
	// Version 4 adds the leader epoch, which is used for fencing.
	//
	// Version 5 adds a new error code, OFFSET_NOT_AVAILABLE.
	Version int16

	// The duration in milliseconds for which the request was throttled due to a quota violation,
	// or zero if the request did not violate any quota. (Version 2+)
	ThrottleTime time.Duration

	Topics []ListOffsetTopicResponse
}

func (r *ListOffsetResponse) encode(pe packetEncoder) error {
	pe.putInt32(int32(r.ThrottleTime / time.Millisecond))

	if err := pe.putArrayLength(len(r.Topics)); err != nil {
		return err
	}

	for _, topic := range r.Topics {
		if err := topic.encode(pe, r.Version); err != nil {
			return err
		}
	}

	return nil
}

func (r *ListOffsetResponse) decode(pd packetDecoder, version int16) error {
	throttleTime, err := pd.getInt32()
	if err != nil {
		return err
	}
	r.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	length, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	topics := make([]ListOffsetTopicResponse, length)
	for i := 0; i < length; i++ {
		topic := ListOffsetTopicResponse{}
		if err := topic.decode(pd, version); err != nil {
			return err
		}
		topics[i] = topic
	}

	return nil
}

func (r *ListOffsetResponse) key() int16 {
	return 2
}

func (r *ListOffsetResponse) version() int16 {
	return 0
}

func (r *ListOffsetResponse) requiredVersion() KafkaVersion {
	switch r.Version {
	case 0:
		return V0_8_2_0
	case 1:
		return V0_10_1_0
	case 2:
		return V0_11_0_0
	case 3:
		return V2_0_0_0
	case 4:
		return V2_1_0_0
	case 5:
		return V2_2_0_0
	default:
		return MinVersion
	}
}

type ListOffsetTopicResponse struct {
	Topic      string
	Partitions []ListOffsetPartitionResponse
}

func (r *ListOffsetTopicResponse) encode(pe packetEncoder, version int16) error {
	if err := pe.putString(r.Topic); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(r.Partitions)); err != nil {
		return err
	}

	for _, partition := range r.Partitions {
		if err := partition.encode(pe, version); err != nil {
			return err
		}
	}

	return nil
}

func (r *ListOffsetTopicResponse) decode(pd packetDecoder, version int16) error {
	var err error
	r.Topic, err = pd.getString()
	if err != nil {
		return err
	}

	length, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	res := make([]ListOffsetPartitionResponse, length)
	for i := 0; i < length; i++ {
		pres := ListOffsetPartitionResponse{}
		if err := pres.decode(pd, version); err != nil {
			return err
		}
		res[i] = pres
	}

	return nil
}

type ListOffsetPartitionResponse struct {
	PartitionID int32
	ErrorCode   KError

	// The result offsets. (only set on version 0)
	OldStyleOffsets []int64

	// The timestamp associated with the returned offset (version 1+).
	Timestamp Timestamp

	// The returned offset.
	Offset int64

	// Only set for version 4+
	LeaderEpoch int32
}

func (r *ListOffsetPartitionResponse) encode(pe packetEncoder, version int16) error {
	pe.putInt32(r.PartitionID)
	pe.putInt8(int8(r.ErrorCode))

	if version == 0 {
		if err := pe.putInt64Array(r.OldStyleOffsets); err != nil {
			return err
		}
	}

	if version >= 1 {
		err := r.Timestamp.encode(pe)
		if err != nil {
			return err
		}

		pe.putInt64(r.Offset)
	}

	if version >= 4 {
		pe.putInt32(r.LeaderEpoch)
	}

	return nil
}

func (r *ListOffsetPartitionResponse) decode(pd packetDecoder, version int16) error {
	var err error
	r.PartitionID, err = pd.getInt32()
	if err != nil {
		return err
	}

	errorCode, err := pd.getInt8()
	if err != nil {
		return err
	}
	r.ErrorCode = KError(errorCode)

	if version == 0 {
		r.OldStyleOffsets, err = pd.getInt64Array()
		if err != nil {
			return err
		}
	}

	if version >= 1 {
		ts := Timestamp{}
		err := ts.decode(pd)
		if err != nil {
			return err
		}
		r.Timestamp = ts

		r.Offset, err = pd.getInt64()
		if err != nil {
			return err
		}
	}

	if version >= 4 {
		r.LeaderEpoch, err = pd.getInt32()
		if err != nil {
			return err
		}
	}

	return nil
}
