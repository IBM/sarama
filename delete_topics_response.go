package sarama

import (
	"time"
)

type DeleteTopicsResponse struct {
	Version         int16
	ThrottleTime    time.Duration
	TopicErrorCodes map[string]KError
}

func (d *DeleteTopicsResponse) setVersion(v int16) {
	d.Version = v
}

func (d *DeleteTopicsResponse) encode(pe packetEncoder) error {
	pe.setFlexible(d.Version >= 4)
	if d.Version >= 1 {
		pe.putInt32(int32(d.ThrottleTime / time.Millisecond))
	}

	if err := pe.putArrayLength(len(d.TopicErrorCodes)); err != nil {
		return err
	}
	for topic, errorCode := range d.TopicErrorCodes {
		if err := pe.putString(topic); err != nil {
			return err
		}
		pe.putInt16(int16(errorCode))
		pe.maybePutEmptyTaggedFieldArray()
	}

	pe.maybePutEmptyTaggedFieldArray()
	return nil
}

func (d *DeleteTopicsResponse) decode(pd packetDecoder, version int16) (err error) {
	pd.setFlexible(version >= 4)
	if version >= 1 {
		throttleTime, err := pd.getInt32()
		if err != nil {
			return err
		}
		d.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

		d.Version = version
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	d.TopicErrorCodes = make(map[string]KError, n)

	for i := 0; i < n; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		errorCode, err := pd.getInt16()
		if err != nil {
			return err
		}

		d.TopicErrorCodes[topic] = KError(errorCode)
		if _, err := pd.maybeGetEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}

	if _, err := pd.maybeGetEmptyTaggedFieldArray(); err != nil {
		return err
	}
	return nil
}

func (d *DeleteTopicsResponse) key() int16 {
	return apiKeyDeleteTopics
}

func (d *DeleteTopicsResponse) version() int16 {
	return d.Version
}

func (d *DeleteTopicsResponse) headerVersion() int16 {
	if d.Version >= 4 {
		return 1
	}
	return 0
}

func (d *DeleteTopicsResponse) isValidVersion() bool {
	return d.Version >= 0 && d.Version <= 4
}

func (d *DeleteTopicsResponse) requiredVersion() KafkaVersion {
	switch d.Version {
	case 4:
		return V2_4_0_0
	case 3:
		return V2_1_0_0
	case 2:
		return V2_0_0_0
	case 1:
		return V0_11_0_0
	case 0:
		return V0_10_1_0
	default:
		return V2_2_0_0
	}
}

func (r *DeleteTopicsResponse) throttleTime() time.Duration {
	return r.ThrottleTime
}
