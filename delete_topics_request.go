package sarama

type DeleteTopicsRequest struct {
	Topics  []string
	Timeout int32
}

func (r *DeleteTopicsRequest) encode(e packetEncoder) error {
	if err := e.putStringArray(r.Topics); err != nil {
		return err
	}
	e.putInt32(r.Timeout)
	return nil
}

func (r *DeleteTopicsRequest) decode(d packetDecoder, version int16) error {
	var err error
	r.Topics, err = d.getStringArray()
	if err != nil {
		return err
	}
	r.Timeout, err = d.getInt32()
	return err
}

func (r *DeleteTopicsRequest) key() int16 {
	return 20
}

func (r *DeleteTopicsRequest) version() int16 {
	return 0
}

func (r *DeleteTopicsRequest) requiredVersion() KafkaVersion {
	return minVersion
}
