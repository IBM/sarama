package sarama

//GroupProtocol is a group protocol type
type GroupProtocol struct {
	Name     string
	Metadata []byte
}

func (g *GroupProtocol) decode(pd packetDecoder) (err error) {
	g.Name, err = pd.getString()
	if err != nil {
		return err
	}
	g.Metadata, err = pd.getBytes()
	return err
}

func (g *GroupProtocol) encode(pe packetEncoder) (err error) {
	if err := pe.putString(g.Name); err != nil {
		return err
	}
	if err := pe.putBytes(g.Metadata); err != nil {
		return err
	}
	return nil
}

//JoinGroupRequest is a join group request
type JoinGroupRequest struct {
	Version               int16
	GroupID               string
	SessionTimeout        int32
	RebalanceTimeout      int32
	MemberID              string
	ProtocolType          string
	GroupProtocols        map[string][]byte // deprecated; use OrderedGroupProtocols
	OrderedGroupProtocols []*GroupProtocol
}

func (j *JoinGroupRequest) encode(pe packetEncoder) error {
	if err := pe.putString(j.GroupID); err != nil {
		return err
	}
	pe.putInt32(j.SessionTimeout)
	if j.Version >= 1 {
		pe.putInt32(j.RebalanceTimeout)
	}
	if err := pe.putString(j.MemberID); err != nil {
		return err
	}
	if err := pe.putString(j.ProtocolType); err != nil {
		return err
	}

	if len(j.GroupProtocols) > 0 {
		if len(j.OrderedGroupProtocols) > 0 {
			return PacketDecodingError{"cannot specify both GroupProtocols and OrderedGroupProtocols on JoinGroupRequest"}
		}

		if err := pe.putArrayLength(len(j.GroupProtocols)); err != nil {
			return err
		}
		for name, metadata := range j.GroupProtocols {
			if err := pe.putString(name); err != nil {
				return err
			}
			if err := pe.putBytes(metadata); err != nil {
				return err
			}
		}
	} else {
		if err := pe.putArrayLength(len(j.OrderedGroupProtocols)); err != nil {
			return err
		}
		for _, protocol := range j.OrderedGroupProtocols {
			if err := protocol.encode(pe); err != nil {
				return err
			}
		}
	}

	return nil
}

func (j *JoinGroupRequest) decode(pd packetDecoder, version int16) (err error) {
	j.Version = version

	if j.GroupID, err = pd.getString(); err != nil {
		return
	}

	if j.SessionTimeout, err = pd.getInt32(); err != nil {
		return
	}

	if version >= 1 {
		if j.RebalanceTimeout, err = pd.getInt32(); err != nil {
			return err
		}
	}

	if j.MemberID, err = pd.getString(); err != nil {
		return
	}

	if j.ProtocolType, err = pd.getString(); err != nil {
		return
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	j.GroupProtocols = make(map[string][]byte)
	for i := 0; i < n; i++ {
		protocol := &GroupProtocol{}
		if err := protocol.decode(pd); err != nil {
			return err
		}
		j.GroupProtocols[protocol.Name] = protocol.Metadata
		j.OrderedGroupProtocols = append(j.OrderedGroupProtocols, protocol)
	}

	return nil
}

func (j *JoinGroupRequest) key() int16 {
	return 11
}

func (j *JoinGroupRequest) version() int16 {
	return j.Version
}

func (j *JoinGroupRequest) requiredVersion() KafkaVersion {
	switch j.Version {
	case 2:
		return V0_11_0_0
	case 1:
		return V0_10_1_0
	default:
		return V0_9_0_0
	}
}

//AddGroupProtocol is used to add group protocol
func (j *JoinGroupRequest) AddGroupProtocol(name string, metadata []byte) {
	j.OrderedGroupProtocols = append(j.OrderedGroupProtocols, &GroupProtocol{
		Name:     name,
		Metadata: metadata,
	})
}

//AddGroupProtocolMetadata is used to add group protocol metadata
func (j *JoinGroupRequest) AddGroupProtocolMetadata(name string, metadata *ConsumerGroupMemberMetadata) error {
	bin, err := encode(metadata, nil)
	if err != nil {
		return err
	}

	j.AddGroupProtocol(name, bin)
	return nil
}
