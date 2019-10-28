package sarama

type ListOffsetRequest struct {
	Version int16

	// Broker id of the follower. For normal consumers, use -1.
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

	return nil
}

func (r *ListOffsetRequest) decode(pd packetDecoder, version int16) (err error) {
	return
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
	default:
		return MinVersion
	}
}

type TopicOffsetsRequest struct {
	Topic string

	// The current leader epoch, if provided, is used to fence consumers/replicas with old metadata.
	CurrentLeaderEpoch int32
}

func (r *TopicOffsetsRequest) encode(pe packetEncoder, version int16) error {
	if err := pe.putString(r.Topic); err != nil {
		return err
	}

	return nil
}

func (r *TopicOffsetsRequest) decode(pd packetDecoder, version int16) (err error) {
	return
}

type PartitionOffsetRequest struct {
	PartitionID int32

	// Used to ask for all messages before a certain time (ms). There are two special values.
	// Specify -1 to receive the latest offset (i.e. the offset of the next coming message)
	// and -2 to receive the earliest available offset.
	Timestamp Timestamp

	// Maximum offsets to return (removed in V1)
	MaxNumOffsets int32
}
