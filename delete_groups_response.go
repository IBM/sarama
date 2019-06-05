package sarama

import (
	"time"
)

//DeleteGroupsResponse is used to create a delete groups response
type DeleteGroupsResponse struct {
	ThrottleTime    time.Duration
	GroupErrorCodes map[string]KError
}

func (d *DeleteGroupsResponse) encode(pe packetEncoder) error {
	pe.putInt32(int32(d.ThrottleTime / time.Millisecond))

	if err := pe.putArrayLength(len(d.GroupErrorCodes)); err != nil {
		return err
	}
	for groupID, errorCode := range d.GroupErrorCodes {
		if err := pe.putString(groupID); err != nil {
			return err
		}
		pe.putInt16(int16(errorCode))
	}

	return nil
}

func (d *DeleteGroupsResponse) decode(pd packetDecoder, version int16) error {
	throttleTime, err := pd.getInt32()
	if err != nil {
		return err
	}
	d.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	d.GroupErrorCodes = make(map[string]KError, n)
	for i := 0; i < n; i++ {
		groupID, err := pd.getString()
		if err != nil {
			return err
		}
		errorCode, err := pd.getInt16()
		if err != nil {
			return err
		}

		d.GroupErrorCodes[groupID] = KError(errorCode)
	}

	return nil
}

func (d *DeleteGroupsResponse) key() int16 {
	return 42
}

func (d *DeleteGroupsResponse) version() int16 {
	return 0
}

func (d *DeleteGroupsResponse) requiredVersion() KafkaVersion {
	return V1_1_0_0
}
