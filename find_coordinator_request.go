package sarama

type CoordinatorType int8

const (
	CoordinatorGroup CoordinatorType = iota
	CoordinatorTransaction
)

type FindCoordinatorRequest struct {
	Version         int16
	CoordinatorKey  string
	CoordinatorType CoordinatorType
}

func (f *FindCoordinatorRequest) setVersion(v int16) {
	f.Version = v
}

func (f *FindCoordinatorRequest) encode(pe packetEncoder) error {
	if err := pe.putString(f.CoordinatorKey); err != nil {
		return err
	}

	if f.Version >= 1 {
		pe.putInt8(int8(f.CoordinatorType))
	}

	return nil
}

func (f *FindCoordinatorRequest) decode(pd packetDecoder, version int16) (err error) {
	if f.CoordinatorKey, err = pd.getString(); err != nil {
		return err
	}

	if version >= 1 {
		f.Version = version
		coordinatorType, err := pd.getInt8()
		if err != nil {
			return err
		}

		f.CoordinatorType = CoordinatorType(coordinatorType)
	}

	return nil
}

func (f *FindCoordinatorRequest) key() int16 {
	return apiKeyFindCoordinator
}

func (f *FindCoordinatorRequest) version() int16 {
	return f.Version
}

func (r *FindCoordinatorRequest) headerVersion() int16 {
	return 1
}

func (f *FindCoordinatorRequest) isValidVersion() bool {
	return f.Version >= 0 && f.Version <= 2
}

func (f *FindCoordinatorRequest) requiredVersion() KafkaVersion {
	switch f.Version {
	case 2:
		return V2_0_0_0
	case 1:
		return V0_11_0_0
	default:
		return V0_8_2_0
	}
}
