package sarama

import (
	"fmt"

	"github.com/rcrowley/go-metrics"
)

// Encoder is the interface that wraps the basic Encode method.
// Anything implementing Encoder can be turned into bytes using Kafka's encoding rules.
type encoder interface {
	encode(pe packetEncoder) error
}

// Encode takes an Encoder and turns it into bytes.
func encode(e encoder) ([]byte, error) {
	return encodeWithMetrics(e, nil)
}

func encodeWithMetrics(e encoder, metricRegistry metrics.Registry) ([]byte, error) {
	if e == nil {
		return nil, nil
	}

	var prepEnc prepEncoder
	var realEnc realEncoder

	err := e.encode(&prepEnc)
	if err != nil {
		return nil, err
	}

	if prepEnc.length < 0 || prepEnc.length > int(MaxRequestSize) {
		return nil, PacketEncodingError{fmt.Sprintf("invalid request size (%d)", prepEnc.length)}
	}

	recordCountPerTopic := make(map[string]int64)
	if metricRegistry != nil {
		// Register the observer for the batch size
		batchSize := getOrRegisterHistogram("batch-size", metricRegistry)
		realEnc.batchSizeObserver = func(topic string, size int64) {
			topicBatchSize := getOrRegisterTopicHistogram("batch-size", topic, metricRegistry)
			// Histogram do not support decimal values, let's use an integer percentage
			batchSize.Update(size)
			topicBatchSize.Update(size)
		}
		// Register the observer for the record count
		realEnc.recordCountObserver = func(topic string, count int64) {
			// Add message set count together to update the metric in one shot later on
			recordCountPerTopic[topic] = recordCountPerTopic[topic] + count
		}
		// Register the observer for the compression ratio
		compressionRatio := getOrRegisterHistogram("compression-rate", metricRegistry)
		realEnc.compressionRatioObserver = func(topic string, ratio float64) {
			topicCompressionRatio := getOrRegisterTopicHistogram("compression-rate", topic, metricRegistry)
			// Histogram do not support decimal values, let's use an integer percentage
			intRatio := int64(100 * ratio)
			compressionRatio.Update(intRatio)
			topicCompressionRatio.Update(intRatio)
		}
	}

	realEnc.raw = make([]byte, prepEnc.length)
	err = e.encode(&realEnc)
	if err != nil {
		return nil, err
	}

	if metricRegistry != nil {
		totalRecordCount := int64(0)

		for topic, count := range recordCountPerTopic {
			getOrRegisterTopicMeter("record-send-rate", topic, metricRegistry).Mark(count)
			getOrRegisterTopicHistogram("records-per-request", topic, metricRegistry).Update(count)
			totalRecordCount = totalRecordCount + count
		}

		// Only recorded for produce request
		if totalRecordCount > 0 {
			metrics.GetOrRegisterMeter("record-send-rate", metricRegistry).Mark(totalRecordCount)
			getOrRegisterHistogram("records-per-request", metricRegistry).Update(totalRecordCount)
		}
	}

	return realEnc.raw, nil
}

// Decoder is the interface that wraps the basic Decode method.
// Anything implementing Decoder can be extracted from bytes using Kafka's encoding rules.
type decoder interface {
	decode(pd packetDecoder) error
}

type versionedDecoder interface {
	decode(pd packetDecoder, version int16) error
}

// Decode takes bytes and a Decoder and fills the fields of the decoder from the bytes,
// interpreted using Kafka's encoding rules.
func decode(buf []byte, in decoder) error {
	if buf == nil {
		return nil
	}

	helper := realDecoder{raw: buf}
	err := in.decode(&helper)
	if err != nil {
		return err
	}

	if helper.off != len(buf) {
		return PacketDecodingError{"invalid length"}
	}

	return nil
}

func versionedDecode(buf []byte, in versionedDecoder, version int16) error {
	if buf == nil {
		return nil
	}

	helper := realDecoder{raw: buf}
	err := in.decode(&helper, version)
	if err != nil {
		return err
	}

	if helper.off != len(buf) {
		return PacketDecodingError{"invalid length"}
	}

	return nil
}
