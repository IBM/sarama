package sarama

//SaslHandshakeRequest is a SASL handshake request
type SaslHandshakeRequest struct {
	Mechanism string
	Version   int16
}

func (s *SaslHandshakeRequest) encode(pe packetEncoder) error {
	if err := pe.putString(s.Mechanism); err != nil {
		return err
	}

	return nil
}

func (s *SaslHandshakeRequest) decode(pd packetDecoder, version int16) (err error) {
	if s.Mechanism, err = pd.getString(); err != nil {
		return err
	}

	return nil
}

func (s *SaslHandshakeRequest) key() int16 {
	return 17
}

func (s *SaslHandshakeRequest) version() int16 {
	return s.Version
}

func (s *SaslHandshakeRequest) requiredVersion() KafkaVersion {
	return V0_10_0_0
}
