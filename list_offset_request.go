package sarama

// ListOffsetRequest can be sent to get partitions offsets from one or more topics at a specific Time
// or the oldest / latest offsets (low and high watermarks).
type ListOffsetRequest struct {
	// Valid versions: 0 - 5
	//
	// Version 1 removes MaxNumOffsets. From this version forward, only a single
	// offset can be returned.
	//
	// Version 2 adds the isolation level, which is used for transactional reads.
	//
	// Version 3 is the same as version 2.
	//
	// Version 4 adds the current leader epoch, which is used for fencing.
	//
	// Version 5 is the same as version 5.
	Version int16

	// The broker ID of the requestor, or -1 if this request is being made by a normal consumer.
	ReplicaID int32

	// This setting controls the visibility of transactional records (added in V2).
	// Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible.
	// With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible.
	// To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current
	// LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the
	// result, which allows consumers to discard ABORTED transactional records")
	IsolationLevevel IsolationLevel

	OffsetRequests []TopicOffsetsRequest
}

func (r *ListOffsetRequest) encode(pe packetEncoder) error {
	pe.putInt32(r.ReplicaID)
	if r.Version >= 2 {
		pe.putInt8(int8(r.IsolationLevevel))
	}

	for _, req := range r.OffsetRequests {
		if err := req.encode(pe, r.Version); err != nil {
			return err
		}
	}

	return nil
}

func (r *ListOffsetRequest) decode(pd packetDecoder, version int16) (err error) {
	r.ReplicaID, err = pd.getInt32()
	if err != nil {
		return err
	}

	if version >= 2 {
		lvl, err := pd.getInt8()
		if err != nil {
			return err
		}
		r.IsolationLevevel = IsolationLevel(lvl)
	}

	length, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	reqs := make([]TopicOffsetsRequest, length)
	for i := 0; i < length; i++ {
		req := TopicOffsetsRequest{}
		if err := req.decode(pd, version); err != nil {
			return err
		}
		reqs[i] = req
	}

	return nil
}

func (r *ListOffsetRequest) key() int16 {
	return 2
}

func (r *ListOffsetRequest) version() int16 {
	return r.Version
}

func (r *ListOffsetRequest) requiredVersion() KafkaVersion {
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

// TopicOffsetsRequest represents a single topic in the request whose partitions' offsets shall be fetched.
type TopicOffsetsRequest struct {
	Topic      string
	Partitions []PartitionOffsetRequest
}

func (r *TopicOffsetsRequest) encode(pe packetEncoder, version int16) error {
	if err := pe.putString(r.Topic); err != nil {
		return err
	}

	for _, partition := range r.Partitions {
		if err := partition.encode(pe, version); err != nil {
			return err
		}
	}

	return nil
}

func (r *TopicOffsetsRequest) decode(pd packetDecoder, version int16) (err error) {
	r.Topic, err = pd.getString()
	if err != nil {
		return err
	}

	length, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	reqs := make([]PartitionOffsetRequest, length)
	for i := 0; i < length; i++ {
		req := PartitionOffsetRequest{}
		if err := req.decode(pd, version); err != nil {
			return err
		}
		reqs[i] = req
	}

	return
}

// PartitionOffsetRequest represents a single partition in the request whose offsets shall be fetched.
type PartitionOffsetRequest struct {
	// The partition index.
	PartitionID int32

	// The current leader epoch, if provided, is used to fence consumers/replicas with old metadata (version 4+).
	CurrentLeaderEpoch int32

	// Used to ask for all messages before a certain time (ms). There are two special values.
	// Specify -1 to receive the latest offset (i.e. the offset of the next coming message)
	// and -2 to receive the earliest available offset.
	Timestamp Timestamp

	// The maximum number of offsets to report. (removed in V1)
	MaxNumOffsets int32
}

func (r *PartitionOffsetRequest) encode(pe packetEncoder, version int16) error {
	pe.putInt32(r.PartitionID)

	if version >= 4 {
		pe.putInt32(r.CurrentLeaderEpoch)
	}

	err := r.Timestamp.encode(pe)
	if err != nil {
		return err
	}

	if version == 0 {
		pe.putInt32(r.MaxNumOffsets)
	}

	return nil
}

func (r *PartitionOffsetRequest) decode(pd packetDecoder, version int16) (err error) {
	r.PartitionID, err = pd.getInt32()
	if err != nil {
		return err
	}

	if version >= 4 {
		r.CurrentLeaderEpoch, err = pd.getInt32()
		if err != nil {
			return err
		}
	}

	err = r.Timestamp.decode(pd)
	if err != nil {
		return err
	}

	if version == 0 {
		r.MaxNumOffsets, err = pd.getInt32()
		if err != nil {
			return err
		}
	}

	return
}
