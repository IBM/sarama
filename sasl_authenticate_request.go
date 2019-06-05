package sarama

//SaslAuthenticateRequest is SASL authenticate request type
type SaslAuthenticateRequest struct {
	SaslAuthBytes []byte
}

// APIKeySASLAuth is the API key for the SaslAuthenticate Kafka API
const APIKeySASLAuth = 36

func (s *SaslAuthenticateRequest) encode(pe packetEncoder) error {
	return pe.putBytes(s.SaslAuthBytes)
}

func (s *SaslAuthenticateRequest) decode(pd packetDecoder, version int16) (err error) {
	s.SaslAuthBytes, err = pd.getBytes()
	return err
}

func (s *SaslAuthenticateRequest) key() int16 {
	return APIKeySASLAuth
}

func (s *SaslAuthenticateRequest) version() int16 {
	return 0
}

func (s *SaslAuthenticateRequest) requiredVersion() KafkaVersion {
	return V1_0_0_0
}
