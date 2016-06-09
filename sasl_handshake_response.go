package sarama

type SaslHandshakeResponse struct {
	Err               KError
	EnabledMechanisms []string
}

func (r *SaslHandshakeResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(r.Err))
	return pe.putStringArray(r.EnabledMechanisms)
}

func (r *SaslHandshakeResponse) decode(pd packetDecoder) error {
	if kerr, err := pd.getInt16(); err != nil {
		return err
	} else {
		r.Err = KError(kerr)
	}

	var err error
	if r.EnabledMechanisms, err = pd.getStringArray(); err != nil {
		return err
	}

	return nil
}
