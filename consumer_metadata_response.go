package sarama

import (
	"net"
	"strconv"
)

// ConsumerMetadataResponse holds the response for a consumer group meta data requests
type ConsumerMetadataResponse struct {
	Version         int16
	Err             KError
	Coordinator     *Broker
	CoordinatorID   int32  // deprecated: use Coordinator.ID()
	CoordinatorHost string // deprecated: use Coordinator.Addr()
	CoordinatorPort int32  // deprecated: use Coordinator.Addr()
}

func (r *ConsumerMetadataResponse) setVersion(v int16) {
	r.Version = v
}

func (r *ConsumerMetadataResponse) decode(pd packetDecoder, version int16) (err error) {
	tmp := new(FindCoordinatorResponse)

	if err := tmp.decode(pd, version); err != nil {
		return err
	}

	r.Err = tmp.Err

	r.Coordinator = tmp.Coordinator
	if tmp.Coordinator == nil {
		return nil
	}

	// this can all go away in 2.0, but we have to fill in deprecated fields to maintain
	// backwards compatibility
	host, portstr, err := net.SplitHostPort(r.Coordinator.Addr())
	if err != nil {
		return err
	}
	port, err := strconv.ParseInt(portstr, 10, 32)
	if err != nil {
		return err
	}
	r.CoordinatorID = r.Coordinator.ID()
	r.CoordinatorHost = host
	r.CoordinatorPort = int32(port)

	return nil
}

func (r *ConsumerMetadataResponse) encode(pe packetEncoder) error {
	if r.Coordinator == nil {
		r.Coordinator = new(Broker)
		r.Coordinator.id = r.CoordinatorID
		r.Coordinator.addr = net.JoinHostPort(r.CoordinatorHost, strconv.Itoa(int(r.CoordinatorPort)))
	}

	tmp := &FindCoordinatorResponse{
		Version:     r.Version,
		Err:         r.Err,
		Coordinator: r.Coordinator,
	}

	if err := tmp.encode(pe); err != nil {
		return err
	}

	return nil
}

func (r *ConsumerMetadataResponse) key() int16 {
	return apiKeyFindCoordinator
}

func (r *ConsumerMetadataResponse) version() int16 {
	return r.Version
}

func (r *ConsumerMetadataResponse) headerVersion() int16 {
	return 0
}

func (r *ConsumerMetadataResponse) isValidVersion() bool {
	return r.Version >= 0 && r.Version <= 2
}

func (r *ConsumerMetadataResponse) requiredVersion() KafkaVersion {
	switch r.Version {
	case 2:
		return V2_0_0_0
	case 1:
		return V0_11_0_0
	default:
		return V0_8_2_0
	}
}
