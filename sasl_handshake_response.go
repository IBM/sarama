package sarama

//SaslHandshakeResponse is a response for SASL handshake
type SaslHandshakeResponse struct {
	Err               KError
	EnabledMechanisms []string
}

func (s *SaslHandshakeResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(s.Err))
	return pe.putStringArray(s.EnabledMechanisms)
}

func (s *SaslHandshakeResponse) decode(pd packetDecoder, version int16) error {
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}

	s.Err = KError(kerr)

	if s.EnabledMechanisms, err = pd.getStringArray(); err != nil {
		return err
	}

	return nil
}

func (s *SaslHandshakeResponse) key() int16 {
	return 17
}

func (s *SaslHandshakeResponse) version() int16 {
	return 0
}

func (s *SaslHandshakeResponse) requiredVersion() KafkaVersion {
	return V0_10_0_0
}
