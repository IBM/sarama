package sarama

import (
	"sort"
	"time"
)

// response message format is:
// throttleMs(int32) [topic]
// where topic is:
//  name(string) [partition]
// where partition is:
//  id(int32) low_watermark(int64) error_code(int16)

//DeleteRecordsResponse is a delete records response type
type DeleteRecordsResponse struct {
	Version      int16
	ThrottleTime time.Duration
	Topics       map[string]*DeleteRecordsResponseTopic
}

func (d *DeleteRecordsResponse) encode(pe packetEncoder) error {
	pe.putInt32(int32(d.ThrottleTime / time.Millisecond))

	if err := pe.putArrayLength(len(d.Topics)); err != nil {
		return err
	}
	keys := make([]string, 0, len(d.Topics))
	for topic := range d.Topics {
		keys = append(keys, topic)
	}
	sort.Strings(keys)
	for _, topic := range keys {
		if err := pe.putString(topic); err != nil {
			return err
		}
		if err := d.Topics[topic].encode(pe); err != nil {
			return err
		}
	}
	return nil
}

func (d *DeleteRecordsResponse) decode(pd packetDecoder, version int16) error {
	d.Version = version

	throttleTime, err := pd.getInt32()
	if err != nil {
		return err
	}
	d.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	if n > 0 {
		d.Topics = make(map[string]*DeleteRecordsResponseTopic, n)
		for i := 0; i < n; i++ {
			topic, err := pd.getString()
			if err != nil {
				return err
			}
			details := new(DeleteRecordsResponseTopic)
			if err = details.decode(pd, version); err != nil {
				return err
			}
			d.Topics[topic] = details
		}
	}

	return nil
}

func (d *DeleteRecordsResponse) key() int16 {
	return 21
}

func (d *DeleteRecordsResponse) version() int16 {
	return 0
}

func (d *DeleteRecordsResponse) requiredVersion() KafkaVersion {
	return V0_11_0_0
}

//DeleteRecordsResponseTopic is a delete records response for a topic
type DeleteRecordsResponseTopic struct {
	Partitions map[int32]*DeleteRecordsResponsePartition
}

func (d *DeleteRecordsResponseTopic) encode(pe packetEncoder) error {
	if err := pe.putArrayLength(len(d.Partitions)); err != nil {
		return err
	}
	keys := make([]int32, 0, len(d.Partitions))
	for partition := range d.Partitions {
		keys = append(keys, partition)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, partition := range keys {
		pe.putInt32(partition)
		if err := d.Partitions[partition].encode(pe); err != nil {
			return err
		}
	}
	return nil
}

func (d *DeleteRecordsResponseTopic) decode(pd packetDecoder, version int16) error {
	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	if n > 0 {
		d.Partitions = make(map[int32]*DeleteRecordsResponsePartition, n)
		for i := 0; i < n; i++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			details := new(DeleteRecordsResponsePartition)
			if err = details.decode(pd, version); err != nil {
				return err
			}
			d.Partitions[partition] = details
		}
	}

	return nil
}

//DeleteRecordsResponsePartition is a delete records response for partition
type DeleteRecordsResponsePartition struct {
	LowWatermark int64
	Err          KError
}

func (d *DeleteRecordsResponsePartition) encode(pe packetEncoder) error {
	pe.putInt64(d.LowWatermark)
	pe.putInt16(int16(d.Err))
	return nil
}

func (d *DeleteRecordsResponsePartition) decode(pd packetDecoder, version int16) error {
	lowWatermark, err := pd.getInt64()
	if err != nil {
		return err
	}
	d.LowWatermark = lowWatermark

	kErr, err := pd.getInt16()
	if err != nil {
		return err
	}
	d.Err = KError(kErr)

	return nil
}
