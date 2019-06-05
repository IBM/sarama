package sarama

//SaslAuthenticateResponse is SASL authentication response
type SaslAuthenticateResponse struct {
	Err           KError
	ErrorMessage  *string
	SaslAuthBytes []byte
}

func (s *SaslAuthenticateResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(s.Err))
	if err := pe.putNullableString(s.ErrorMessage); err != nil {
		return err
	}
	return pe.putBytes(s.SaslAuthBytes)
}

func (s *SaslAuthenticateResponse) decode(pd packetDecoder, version int16) error {
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}

	s.Err = KError(kerr)

	if s.ErrorMessage, err = pd.getNullableString(); err != nil {
		return err
	}

	s.SaslAuthBytes, err = pd.getBytes()

	return err
}

func (s *SaslAuthenticateResponse) key() int16 {
	return APIKeySASLAuth
}

func (s *SaslAuthenticateResponse) version() int16 {
	return 0
}

func (s *SaslAuthenticateResponse) requiredVersion() KafkaVersion {
	return V1_0_0_0
}
