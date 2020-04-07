package sarama

// FetchPartition contains the partitions to fetch.
type FetchPartition struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// PartitionIndex contains the partition index.
	PartitionIndex int32
	// CurrentLeaderEpoch contains the current leader epoch of the partition.
	CurrentLeaderEpoch int32
	// FetchOffset contains the message offset.
	FetchOffset int64
	// LogStartOffset contains the earliest available offset of the follower replica.  The field is only used when the request is sent by the follower.
	LogStartOffset int64
	// MaxBytes contains the maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may not be honored.
	MaxBytes int32
}

func (f *FetchPartition) encode(pe packetEncoder, version int16) (err error) {
	f.Version = version
	pe.putInt32(f.PartitionIndex)

	if f.Version >= 9 {
		pe.putInt32(f.CurrentLeaderEpoch)
	}

	pe.putInt64(f.FetchOffset)

	if f.Version >= 5 {
		pe.putInt64(f.LogStartOffset)
	}

	pe.putInt32(f.MaxBytes)

	return nil
}

func (f *FetchPartition) decode(pd packetDecoder, version int16) (err error) {
	f.Version = version
	if f.PartitionIndex, err = pd.getInt32(); err != nil {
		return err
	}

	if f.Version >= 9 {
		if f.CurrentLeaderEpoch, err = pd.getInt32(); err != nil {
			return err
		}
	}

	if f.FetchOffset, err = pd.getInt64(); err != nil {
		return err
	}

	if f.Version >= 5 {
		if f.LogStartOffset, err = pd.getInt64(); err != nil {
			return err
		}
	}

	if f.MaxBytes, err = pd.getInt32(); err != nil {
		return err
	}

	return nil
}

// FetchableTopic contains the topics to fetch.
type FetchableTopic struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// Name contains the name of the topic to fetch.
	Name string
	// FetchPartitions contains the partitions to fetch.
	FetchPartitions []FetchPartition
}

func (t *FetchableTopic) encode(pe packetEncoder, version int16) (err error) {
	t.Version = version
	if err := pe.putString(t.Name); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(t.FetchPartitions)); err != nil {
		return err
	}
	for _, block := range t.FetchPartitions {
		if err := block.encode(pe, t.Version); err != nil {
			return err
		}
	}

	return nil
}

func (t *FetchableTopic) decode(pd packetDecoder, version int16) (err error) {
	t.Version = version
	if t.Name, err = pd.getString(); err != nil {
		return err
	}

	if numFetchPartitions, err := pd.getArrayLength(); err != nil {
		return err
	} else {
		t.FetchPartitions = make([]FetchPartition, numFetchPartitions)
		for i := 0; i < numFetchPartitions; i++ {
			var block FetchPartition
			if err := block.decode(pd, t.Version); err != nil {
				return err
			}
			t.FetchPartitions[i] = block
		}
	}

	return nil
}

// ForgottenTopic contains in an incremental fetch request, the partitions to remove.
type ForgottenTopic struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// Name contains the partition name.
	Name string
	// ForgottenPartitionIndexes contains the partitions indexes to forget.
	ForgottenPartitionIndexes []int32
}

func (f *ForgottenTopic) encode(pe packetEncoder, version int16) (err error) {
	f.Version = version
	if f.Version >= 7 {
		if err := pe.putString(f.Name); err != nil {
			return err
		}
	}

	if f.Version >= 7 {
		if err := pe.putInt32Array(f.ForgottenPartitionIndexes); err != nil {
			return err
		}
	}

	return nil
}

func (f *ForgottenTopic) decode(pd packetDecoder, version int16) (err error) {
	f.Version = version
	if f.Version >= 7 {
		if f.Name, err = pd.getString(); err != nil {
			return err
		}
	}

	if f.Version >= 7 {
		if f.ForgottenPartitionIndexes, err = pd.getInt32Array(); err != nil {
			return err
		}
	}

	return nil
}

const (
	ReadUncommitted int8 = iota
	ReadCommitted
)

type FetchRequest struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ReplicaID contains the broker ID of the follower, of -1 if this request is from a consumer.
	ReplicaID int32
	// MaxWait contains the maximum time in milliseconds to wait for the response.
	MaxWait int32
	// MinBytes contains the minimum bytes to accumulate in the response.
	MinBytes int32
	// MaxBytes contains the maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.
	MaxBytes int32
	// IsolationLevel contains a This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records
	IsolationLevel int8
	// SessionID contains the fetch session ID.
	SessionID int32
	// Epoch contains the epoch of the partition leader as known to the follower replica or a consumer.
	Epoch int32
	// Topics contains the topics to fetch.
	Topics []FetchableTopic
	// Forgotten contains in an incremental fetch request, the partitions to remove.
	Forgotten []ForgottenTopic
	// RackID contains a Rack ID of the consumer making this request
	RackID string
}

func (r *FetchRequest) encode(pe packetEncoder) (err error) {
	pe.putInt32(r.ReplicaID)

	pe.putInt32(r.MaxWait)

	pe.putInt32(r.MinBytes)

	if r.Version >= 3 {
		pe.putInt32(r.MaxBytes)
	}

	if r.Version >= 4 {
		pe.putInt8(r.IsolationLevel)
	}

	if r.Version >= 7 {
		pe.putInt32(r.SessionID)
	}

	if r.Version >= 7 {
		pe.putInt32(r.Epoch)
	}

	if err := pe.putArrayLength(len(r.Topics)); err != nil {
		return err
	}
	for _, block := range r.Topics {
		if err := block.encode(pe, r.Version); err != nil {
			return err
		}
	}

	if r.Version >= 7 {
		if err := pe.putArrayLength(len(r.Forgotten)); err != nil {
			return err
		}
		for _, block := range r.Forgotten {
			if err := block.encode(pe, r.Version); err != nil {
				return err
			}
		}
	}

	if r.Version >= 11 {
		if err := pe.putString(r.RackID); err != nil {
			return err
		}
	}

	return nil
}

func (r *FetchRequest) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version
	if r.ReplicaID, err = pd.getInt32(); err != nil {
		return err
	}

	if r.MaxWait, err = pd.getInt32(); err != nil {
		return err
	}

	if r.MinBytes, err = pd.getInt32(); err != nil {
		return err
	}

	if r.Version >= 3 {
		if r.MaxBytes, err = pd.getInt32(); err != nil {
			return err
		}
	}

	if r.Version >= 4 {
		if r.IsolationLevel, err = pd.getInt8(); err != nil {
			return err
		}
	}

	if r.Version >= 7 {
		if r.SessionID, err = pd.getInt32(); err != nil {
			return err
		}
	}

	if r.Version >= 7 {
		if r.Epoch, err = pd.getInt32(); err != nil {
			return err
		}
	}

	if numTopics, err := pd.getArrayLength(); err != nil {
		return err
	} else {
		r.Topics = make([]FetchableTopic, numTopics)
		for i := 0; i < numTopics; i++ {
			var block FetchableTopic
			if err := block.decode(pd, r.Version); err != nil {
				return err
			}
			r.Topics[i] = block
		}
	}

	if r.Version >= 7 {
		if numForgotten, err := pd.getArrayLength(); err != nil {
			return err
		} else {
			r.Forgotten = make([]ForgottenTopic, numForgotten)
			for i := 0; i < numForgotten; i++ {
				var block ForgottenTopic
				if err := block.decode(pd, r.Version); err != nil {
					return err
				}
				r.Forgotten[i] = block
			}
		}
	}

	if r.Version >= 11 {
		if r.RackID, err = pd.getString(); err != nil {
			return err
		}
	}

	return nil
}

func (r *FetchRequest) key() int16 {
	return 1
}

func (r *FetchRequest) version() int16 {
	return r.Version
}

func (r *FetchRequest) headerVersion() int16 {
	return 1
}

func (r *FetchRequest) requiredVersion() KafkaVersion {
	switch r.Version {
	case 0:
		return MinVersion
	case 1:
		return V0_9_0_0
	case 2:
		return V0_10_0_0
	case 3:
		return V0_10_1_0
	case 4, 5:
		return V0_11_0_0
	case 6:
		return V1_0_0_0
	case 7:
		return V1_1_0_0
	case 8:
		return V2_0_0_0
	case 9, 10:
		return V2_1_0_0
	case 11:
		return V2_3_0_0
	default:
		return MaxVersion
	}
}
