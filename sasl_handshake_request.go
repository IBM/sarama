package sarama

import "fmt"

type SaslHandshakeRequest struct {
	Mechanism string
	Version   int16
}

func (r *SaslHandshakeRequest) encode(pe packetEncoder) error {
	if err := pe.putString(r.Mechanism); err != nil {
		return err
	}

	return nil
}

func (r *SaslHandshakeRequest) decode(pd packetDecoder, version int16) (err error) {
	if r.Mechanism, err = pd.getString(); err != nil {
		return err
	}

	return nil
}

func (r *SaslHandshakeRequest) key() int16 {
	return 17
}

func (r *SaslHandshakeRequest) version() int16 {
	return r.Version
}

func (r *SaslHandshakeRequest) headerVersion() int16 {
	return 1
}

func (r *SaslHandshakeRequest) isValidVersion() bool {
	return r.Version >= 0 && r.Version <= 1
}

func (r *SaslHandshakeRequest) requiredVersion() KafkaVersion {
	switch r.Version {
	case 1:
		return V1_0_0_0
	default:
		return V0_10_0_0
	}
}

func (r *SaslHandshakeRequest) restrictApiVersion(minVersion, maxVersion int16) error {
	if r.Version < minVersion {
		return fmt.Errorf("%w: %T: unsupported API version %d, supported versions are %d-%d",
			ErrUnsupportedVersion, r, r.Version, minVersion, maxVersion)
	}
	r.Version = max(r.Version, maxVersion)
	return nil
}
