package sarama

type DescribeQuorumResponse struct {
	Version int16

	ErrorCode KError

	// ErrorMessage is the top level error message (v2+)
	ErrorMessage *string

	// Topics contains the per-topic results
	Topics []DescribeQuorumResponseTopic

	// Nodes contains the listeners of the nodes in the quorum (v2+)
	Nodes []DescribeQuorumResponseNode
}

func (r *DescribeQuorumResponse) setVersion(v int16) {
	r.Version = v
}

type DescribeQuorumResponseTopic struct {
	// TopicName is the topic name
	TopicName string

	// Partitions contains the per-partition results
	Partitions []DescribeQuorumResponsePartition
}

type DescribeQuorumResponsePartition struct {
	// PartitionIndex is the partition index
	PartitionIndex int32

	ErrorCode KError

	// ErrorMessage is the partition error message (v2+)
	ErrorMessage *string

	// LeaderID is the id of the current leader, or -1 if the leader is unknown
	LeaderID int32

	// LeaderEpoch is the latest known leader epoch
	LeaderEpoch int32

	// HighWatermark is the high watermark
	HighWatermark int64

	// CurrentVoters is the set of voters of the partition
	CurrentVoters []QuorumReplicaState

	// Observers is the set of observers of the partition
	Observers []QuorumReplicaState
}

type QuorumReplicaState struct {
	// ReplicaID is the id of the replica
	ReplicaID int32

	// ReplicaDirectoryID is the directory id of the replica (v2+)
	ReplicaDirectoryID Uuid

	// LogEndOffset is the last known log end offset of the replica, or -1 if
	// it is unknown
	LogEndOffset int64

	// LastFetchTimestamp is the leader wall clock time in milliseconds when
	// the replica last fetched from the leader, or -1 for the leader itself or
	// if it is unknown (v1+)
	LastFetchTimestamp int64

	// LastCaughtUpTimestamp is the leader wall clock append time in
	// milliseconds of the offset the replica last fetched, or -1 if it is
	// unknown (v1+)
	LastCaughtUpTimestamp int64
}

type DescribeQuorumResponseNode struct {
	// NodeID is the id of the node
	NodeID int32

	// Listeners is the set of listeners of the node
	Listeners []DescribeQuorumResponseListener
}

type DescribeQuorumResponseListener struct {
	// Name is the name of the endpoint
	Name string

	// Host is the hostname
	Host string

	// Port is the port
	Port uint16
}

func (s *QuorumReplicaState) encode(pe packetEncoder, version int16) error {
	pe.putInt32(s.ReplicaID)

	if version >= 2 {
		if err := pe.putUuid(s.ReplicaDirectoryID); err != nil {
			return err
		}
	}

	pe.putInt64(s.LogEndOffset)

	if version >= 1 {
		pe.putInt64(s.LastFetchTimestamp)
		pe.putInt64(s.LastCaughtUpTimestamp)
	}

	pe.putEmptyTaggedFieldArray()
	return nil
}

func (s *QuorumReplicaState) decode(pd packetDecoder, version int16) (err error) {
	if s.ReplicaID, err = pd.getInt32(); err != nil {
		return err
	}

	if version >= 2 {
		if s.ReplicaDirectoryID, err = pd.getUuid(); err != nil {
			return err
		}
	}

	if s.LogEndOffset, err = pd.getInt64(); err != nil {
		return err
	}

	if version >= 1 {
		if s.LastFetchTimestamp, err = pd.getInt64(); err != nil {
			return err
		}
		if s.LastCaughtUpTimestamp, err = pd.getInt64(); err != nil {
			return err
		}
	} else {
		s.LastFetchTimestamp = -1
		s.LastCaughtUpTimestamp = -1
	}

	_, err = pd.getEmptyTaggedFieldArray()
	return err
}

func encodeQuorumReplicaStates(pe packetEncoder, states []QuorumReplicaState, version int16) error {
	if err := pe.putArrayLength(len(states)); err != nil {
		return err
	}
	for i := range states {
		if err := states[i].encode(pe, version); err != nil {
			return err
		}
	}
	return nil
}

func decodeQuorumReplicaStates(pd packetDecoder, version int16) ([]QuorumReplicaState, error) {
	n, err := pd.getArrayLength()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, errInvalidArrayLength
	}

	states := make([]QuorumReplicaState, n)
	for i := range n {
		if err := states[i].decode(pd, version); err != nil {
			return nil, err
		}
	}
	return states, nil
}

func (p *DescribeQuorumResponsePartition) encode(pe packetEncoder, version int16) error {
	pe.putInt32(p.PartitionIndex)

	pe.putKError(p.ErrorCode)

	if version >= 2 {
		if err := pe.putNullableString(p.ErrorMessage); err != nil {
			return err
		}
	}

	pe.putInt32(p.LeaderID)
	pe.putInt32(p.LeaderEpoch)
	pe.putInt64(p.HighWatermark)

	if err := encodeQuorumReplicaStates(pe, p.CurrentVoters, version); err != nil {
		return err
	}

	if err := encodeQuorumReplicaStates(pe, p.Observers, version); err != nil {
		return err
	}

	pe.putEmptyTaggedFieldArray()
	return nil
}

func (p *DescribeQuorumResponsePartition) decode(pd packetDecoder, version int16) (err error) {
	if p.PartitionIndex, err = pd.getInt32(); err != nil {
		return err
	}

	if p.ErrorCode, err = pd.getKError(); err != nil {
		return err
	}

	if version >= 2 {
		if p.ErrorMessage, err = pd.getNullableString(); err != nil {
			return err
		}
	}

	if p.LeaderID, err = pd.getInt32(); err != nil {
		return err
	}

	if p.LeaderEpoch, err = pd.getInt32(); err != nil {
		return err
	}

	if p.HighWatermark, err = pd.getInt64(); err != nil {
		return err
	}

	if p.CurrentVoters, err = decodeQuorumReplicaStates(pd, version); err != nil {
		return err
	}

	if p.Observers, err = decodeQuorumReplicaStates(pd, version); err != nil {
		return err
	}

	_, err = pd.getEmptyTaggedFieldArray()
	return err
}

func (t *DescribeQuorumResponseTopic) encode(pe packetEncoder, version int16) error {
	if err := pe.putString(t.TopicName); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(t.Partitions)); err != nil {
		return err
	}
	for i := range t.Partitions {
		if err := t.Partitions[i].encode(pe, version); err != nil {
			return err
		}
	}

	pe.putEmptyTaggedFieldArray()
	return nil
}

func (t *DescribeQuorumResponseTopic) decode(pd packetDecoder, version int16) (err error) {
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

	t.Partitions = make([]DescribeQuorumResponsePartition, n)
	for i := range n {
		if err := t.Partitions[i].decode(pd, version); err != nil {
			return err
		}
	}

	_, err = pd.getEmptyTaggedFieldArray()
	return err
}

func (l *DescribeQuorumResponseListener) encode(pe packetEncoder) error {
	if err := pe.putString(l.Name); err != nil {
		return err
	}

	if err := pe.putString(l.Host); err != nil {
		return err
	}

	pe.putInt16(int16(l.Port))

	pe.putEmptyTaggedFieldArray()
	return nil
}

func (l *DescribeQuorumResponseListener) decode(pd packetDecoder) (err error) {
	if l.Name, err = pd.getString(); err != nil {
		return err
	}

	if l.Host, err = pd.getString(); err != nil {
		return err
	}

	port, err := pd.getInt16()
	if err != nil {
		return err
	}
	l.Port = uint16(port)

	_, err = pd.getEmptyTaggedFieldArray()
	return err
}

func (n *DescribeQuorumResponseNode) encode(pe packetEncoder) error {
	pe.putInt32(n.NodeID)

	if err := pe.putArrayLength(len(n.Listeners)); err != nil {
		return err
	}
	for i := range n.Listeners {
		if err := n.Listeners[i].encode(pe); err != nil {
			return err
		}
	}

	pe.putEmptyTaggedFieldArray()
	return nil
}

func (n *DescribeQuorumResponseNode) decode(pd packetDecoder) (err error) {
	if n.NodeID, err = pd.getInt32(); err != nil {
		return err
	}

	count, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if count < 0 {
		return errInvalidArrayLength
	}

	n.Listeners = make([]DescribeQuorumResponseListener, count)
	for i := range count {
		if err := n.Listeners[i].decode(pd); err != nil {
			return err
		}
	}

	_, err = pd.getEmptyTaggedFieldArray()
	return err
}

func (r *DescribeQuorumResponse) encode(pe packetEncoder) error {
	pe.putKError(r.ErrorCode)

	if r.Version >= 2 {
		if err := pe.putNullableString(r.ErrorMessage); err != nil {
			return err
		}
	}

	if err := pe.putArrayLength(len(r.Topics)); err != nil {
		return err
	}
	for i := range r.Topics {
		if err := r.Topics[i].encode(pe, r.Version); err != nil {
			return err
		}
	}

	if r.Version >= 2 {
		if err := pe.putArrayLength(len(r.Nodes)); err != nil {
			return err
		}
		for i := range r.Nodes {
			if err := r.Nodes[i].encode(pe); err != nil {
				return err
			}
		}
	}

	pe.putEmptyTaggedFieldArray()
	return nil
}

func (r *DescribeQuorumResponse) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version

	if r.ErrorCode, err = pd.getKError(); err != nil {
		return err
	}

	if version >= 2 {
		if r.ErrorMessage, err = pd.getNullableString(); err != nil {
			return err
		}
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if n < 0 {
		return errInvalidArrayLength
	}

	r.Topics = make([]DescribeQuorumResponseTopic, n)
	for i := range n {
		if err := r.Topics[i].decode(pd, version); err != nil {
			return err
		}
	}

	if version >= 2 {
		if n, err = pd.getArrayLength(); err != nil {
			return err
		}
		if n < 0 {
			return errInvalidArrayLength
		}

		r.Nodes = make([]DescribeQuorumResponseNode, n)
		for i := range n {
			if err := r.Nodes[i].decode(pd); err != nil {
				return err
			}
		}
	}

	_, err = pd.getEmptyTaggedFieldArray()
	return err
}

func (r *DescribeQuorumResponse) key() int16 {
	return apiKeyDescribeQuorum
}

func (r *DescribeQuorumResponse) version() int16 {
	return r.Version
}

func (r *DescribeQuorumResponse) headerVersion() int16 {
	return 1
}

func (r *DescribeQuorumResponse) isValidVersion() bool {
	return r.Version >= 0 && r.Version <= 2
}

func (r *DescribeQuorumResponse) isFlexible() bool {
	return r.isFlexibleVersion(r.Version)
}

func (r *DescribeQuorumResponse) isFlexibleVersion(version int16) bool {
	return version >= 0
}

func (r *DescribeQuorumResponse) requiredVersion() KafkaVersion {
	switch r.Version {
	case 2:
		return V3_9_0_0
	case 1:
		return V3_3_0_0
	default:
		return V2_7_0_0
	}
}
