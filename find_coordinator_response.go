package sarama

import (
	"net"
	"strconv"
	"time"
)

type FindCoordinatorResponse struct {
	Version         int16
	ThrottleTime    time.Duration
	Err             KError
	ErrMsg          *string
	Coordinator     *Broker
	CoordinatorID   int32  // deprecated: use Coordinator.ID()
	CoordinatorHost string // deprecated: use Coordinator.Addr()
	CoordinatorPort int32  // deprecated: use Coordinator.Addr()
}

func (f *FindCoordinatorResponse) decode(pd packetDecoder, version int16) (err error) {
	if version >= 1 {
		f.Version = version

		throttleTime, err := pd.getInt32()
		if err != nil {
			return err
		}
		f.ThrottleTime = time.Duration(throttleTime) * time.Millisecond
	}

	tmp, err := pd.getInt16()
	if err != nil {
		return err
	}
	f.Err = KError(tmp)

	if version >= 1 {
		if f.ErrMsg, err = pd.getNullableString(); err != nil {
			return err
		}
	}

	coordinator := new(Broker)
	if err := coordinator.decode(pd); err != nil {
		return err
	}
	if coordinator.addr == ":0" {
		return nil
	}
	f.Coordinator = coordinator

	// this can all go away in 2.0, but we have to fill in deprecated fields to maintain
	// backwards compatibility
	host, portstr, err := net.SplitHostPort(f.Coordinator.Addr())
	if err != nil {
		return err
	}
	port, err := strconv.ParseInt(portstr, 10, 32)
	if err != nil {
		return err
	}
	f.CoordinatorID = f.Coordinator.ID()
	f.CoordinatorHost = host
	f.CoordinatorPort = int32(port)

	return nil
}

func (f *FindCoordinatorResponse) encode(pe packetEncoder) error {
	if f.Version >= 1 {
		pe.putInt32(int32(f.ThrottleTime / time.Millisecond))
	}

	pe.putInt16(int16(f.Err))

	if f.Version >= 1 {
		if err := pe.putNullableString(f.ErrMsg); err != nil {
			return err
		}
	}

	if f.Coordinator != nil {
		host, portstr, err := net.SplitHostPort(f.Coordinator.Addr())
		if err != nil {
			return err
		}
		port, err := strconv.ParseInt(portstr, 10, 32)
		if err != nil {
			return err
		}
		pe.putInt32(f.Coordinator.ID())
		if err := pe.putString(host); err != nil {
			return err
		}
		pe.putInt32(int32(port))
		return nil
	}
	pe.putInt32(f.CoordinatorID)
	if err := pe.putString(f.CoordinatorHost); err != nil {
		return err
	}
	pe.putInt32(f.CoordinatorPort)
	return nil
}

func (f *FindCoordinatorResponse) key() int16 {
	return 10
}

func (f *FindCoordinatorResponse) version() int16 {
	return f.Version
}

func (f *FindCoordinatorResponse) requiredVersion() KafkaVersion {
	switch f.Version {
	case 1:
		return V0_11_0_0
	default:
		return V0_8_2_0
	}
}
