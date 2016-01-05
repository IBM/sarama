package sarama

type JoinGroupResponse struct {
	Err           KError
	GenerationId  int32
	GroupProtocol string
	LeaderId      string
	MemberId      string
	Members       map[string]*ProtocolMetadata
}

func (r *JoinGroupResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(r.Err))
	pe.putInt32(r.GenerationId)

	if err := pe.putString(r.GroupProtocol); err != nil {
		return err
	}
	if err := pe.putString(r.LeaderId); err != nil {
		return err
	}
	if err := pe.putString(r.MemberId); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(r.Members)); err != nil {
		return err
	}

	for memberId, memberMetadata := range r.Members {
		if err := pe.putString(memberId); err != nil {
			return err
		}

		if data, err := encode(memberMetadata); err != nil {
			return nil
		} else {
			if err := pe.putBytes(data); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *JoinGroupResponse) decode(pd packetDecoder) (err error) {
	if kerr, err := pd.getInt16(); err != nil {
		return err
	} else {
		r.Err = KError(kerr)
	}

	if r.GenerationId, err = pd.getInt32(); err != nil {
		return
	}

	if r.GroupProtocol, err = pd.getString(); err != nil {
		return
	}

	if r.LeaderId, err = pd.getString(); err != nil {
		return
	}

	if r.MemberId, err = pd.getString(); err != nil {
		return
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	r.Members = make(map[string]*ProtocolMetadata)
	for i := 0; i < n; i++ {
		memberId, err := pd.getString()
		if err != nil {
			return err
		}

		memberMetadata, err := pd.getBytes()
		if err != nil {
			return err
		}

		pm := new(ProtocolMetadata)
		if err := decode(memberMetadata, pm); err != nil {
			return nil
		}

		r.Members[memberId] = pm
	}

	return nil
}
