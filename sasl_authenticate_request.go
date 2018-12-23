package sarama

type SaslAuthenticateRequest struct {
	SaslAuthBytes []byte
}

func (r *SaslAuthenticateRequest) encode(pe packetEncoder) error {
	if err := pe.putBytes(r.SaslAuthBytes); err != nil {
		return err
	}

	return nil
}

func (r *SaslAuthenticateRequest) decode(pd packetDecoder, version int16) (err error) {
	if r.SaslAuthBytes, err = pd.getBytes(); err != nil {
		return err
	}

	return nil
}

func (r *SaslAuthenticateRequest) key() int16 {
	return 36
}

func (r *SaslAuthenticateRequest) version() int16 {
	return 0
}

func (r *SaslAuthenticateRequest) requiredVersion() KafkaVersion {
	return V1_0_0_0
}
