package sarama

import "github.com/rcrowley/go-metrics"

// RequiredAcks is used in Produce Requests to tell the broker how many replica acknowledgements
// it must see before responding. Any of the constants defined here are valid. On broker versions
// prior to 0.8.2.0 any other positive int16 is also valid (the broker will wait for that many
// acknowledgements) but in 0.8.2.0 and later this will raise an exception (it has been replaced
// by setting the `min.isr` value in the brokers configuration).
type RequiredAcks int16

const (
	// NoResponse doesn't send any response, the TCP ACK is all you get.
	NoResponse RequiredAcks = 0
	// WaitForLocal waits for only the local commit to succeed before responding.
	WaitForLocal RequiredAcks = 1
	// WaitForAll waits for all in-sync replicas to commit before responding.
	// The minimum number of in-sync replicas is configured on the broker via
	// the `min.insync.replicas` configuration key.
	WaitForAll RequiredAcks = -1
)

//ProduceRequest is a produce request
type ProduceRequest struct {
	TransactionalID *string
	RequiredAcks    RequiredAcks
	Timeout         int32
	Version         int16 // v1 requires Kafka 0.9, v2 requires Kafka 0.10, v3 requires Kafka 0.11
	records         map[string]map[int32]Records
}

func updateMsgSetMetrics(msgSet *MessageSet, compressionRatioMetric metrics.Histogram,
	topicCompressionRatioMetric metrics.Histogram) int64 {
	var topicRecordCount int64
	for _, messageBlock := range msgSet.Messages {
		// Is this a fake "message" wrapping real messages?
		if messageBlock.Msg.Set != nil {
			topicRecordCount += int64(len(messageBlock.Msg.Set.Messages))
		} else {
			// A single uncompressed message
			topicRecordCount++
		}
		// Better be safe than sorry when computing the compression ratio
		if messageBlock.Msg.compressedSize != 0 {
			compressionRatio := float64(len(messageBlock.Msg.Value)) /
				float64(messageBlock.Msg.compressedSize)
			// Histogram do not support decimal values, let's multiple it by 100 for better precision
			intCompressionRatio := int64(100 * compressionRatio)
			compressionRatioMetric.Update(intCompressionRatio)
			topicCompressionRatioMetric.Update(intCompressionRatio)
		}
	}
	return topicRecordCount
}

func updateBatchMetrics(recordBatch *RecordBatch, compressionRatioMetric metrics.Histogram,
	topicCompressionRatioMetric metrics.Histogram) int64 {
	if recordBatch.compressedRecords != nil {
		compressionRatio := int64(float64(recordBatch.recordsLen) / float64(len(recordBatch.compressedRecords)) * 100)
		compressionRatioMetric.Update(compressionRatio)
		topicCompressionRatioMetric.Update(compressionRatio)
	}

	return int64(len(recordBatch.Records))
}

func (p *ProduceRequest) encode(pe packetEncoder) error {
	if p.Version >= 3 {
		if err := pe.putNullableString(p.TransactionalID); err != nil {
			return err
		}
	}
	pe.putInt16(int16(p.RequiredAcks))
	pe.putInt32(p.Timeout)
	metricRegistry := pe.metricRegistry()
	var batchSizeMetric metrics.Histogram
	var compressionRatioMetric metrics.Histogram
	if metricRegistry != nil {
		batchSizeMetric = getOrRegisterHistogram("batch-size", metricRegistry)
		compressionRatioMetric = getOrRegisterHistogram("compression-ratio", metricRegistry)
	}
	totalRecordCount := int64(0)

	err := pe.putArrayLength(len(p.records))
	if err != nil {
		return err
	}

	for topic, partitions := range p.records {
		err = pe.putString(topic)
		if err != nil {
			return err
		}
		err = pe.putArrayLength(len(partitions))
		if err != nil {
			return err
		}
		topicRecordCount := int64(0)
		var topicCompressionRatioMetric metrics.Histogram
		if metricRegistry != nil {
			topicCompressionRatioMetric = getOrRegisterTopicHistogram("compression-ratio", topic, metricRegistry)
		}
		for id, records := range partitions {
			startOffset := pe.offset()
			pe.putInt32(id)
			pe.push(&lengthField{})
			err = records.encode(pe)
			if err != nil {
				return err
			}
			err = pe.pop()
			if err != nil {
				return err
			}
			if metricRegistry != nil {
				if p.Version >= 3 {
					topicRecordCount += updateBatchMetrics(records.RecordBatch, compressionRatioMetric, topicCompressionRatioMetric)
				} else {
					topicRecordCount += updateMsgSetMetrics(records.MsgSet, compressionRatioMetric, topicCompressionRatioMetric)
				}
				batchSize := int64(pe.offset() - startOffset)
				batchSizeMetric.Update(batchSize)
				getOrRegisterTopicHistogram("batch-size", topic, metricRegistry).Update(batchSize)
			}
		}
		if topicRecordCount > 0 {
			getOrRegisterTopicMeter("record-send-rate", topic, metricRegistry).Mark(topicRecordCount)
			getOrRegisterTopicHistogram("records-per-request", topic, metricRegistry).Update(topicRecordCount)
			totalRecordCount += topicRecordCount
		}
	}
	if totalRecordCount > 0 {
		metrics.GetOrRegisterMeter("record-send-rate", metricRegistry).Mark(totalRecordCount)
		getOrRegisterHistogram("records-per-request", metricRegistry).Update(totalRecordCount)
	}

	return nil
}

func (p *ProduceRequest) decode(pd packetDecoder, version int16) error {
	p.Version = version

	if version >= 3 {
		id, err := pd.getNullableString()
		if err != nil {
			return err
		}
		p.TransactionalID = id
	}
	requiredAcks, err := pd.getInt16()
	if err != nil {
		return err
	}
	p.RequiredAcks = RequiredAcks(requiredAcks)
	if p.Timeout, err = pd.getInt32(); err != nil {
		return err
	}
	topicCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if topicCount == 0 {
		return nil
	}

	p.records = make(map[string]map[int32]Records)
	for i := 0; i < topicCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		p.records[topic] = make(map[int32]Records)

		for j := 0; j < partitionCount; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			size, err := pd.getInt32()
			if err != nil {
				return err
			}
			recordsDecoder, err := pd.getSubset(int(size))
			if err != nil {
				return err
			}
			var records Records
			if err := records.decode(recordsDecoder); err != nil {
				return err
			}
			p.records[topic][partition] = records
		}
	}

	return nil
}

func (p *ProduceRequest) key() int16 {
	return 0
}

func (p *ProduceRequest) version() int16 {
	return p.Version
}

func (p *ProduceRequest) requiredVersion() KafkaVersion {
	switch p.Version {
	case 1:
		return V0_9_0_0
	case 2:
		return V0_10_0_0
	case 3:
		return V0_11_0_0
	default:
		return MinVersion
	}
}

func (p *ProduceRequest) ensureRecords(topic string, partition int32) {
	if p.records == nil {
		p.records = make(map[string]map[int32]Records)
	}

	if p.records[topic] == nil {
		p.records[topic] = make(map[int32]Records)
	}
}

//AddMessage add a message to produce request
func (p *ProduceRequest) AddMessage(topic string, partition int32, msg *Message) {
	p.ensureRecords(topic, partition)
	set := p.records[topic][partition].MsgSet

	if set == nil {
		set = new(MessageSet)
		p.records[topic][partition] = newLegacyRecords(set)
	}

	set.addMessage(msg)
}

//AddSet adds a message to produce request
func (p *ProduceRequest) AddSet(topic string, partition int32, set *MessageSet) {
	p.ensureRecords(topic, partition)
	p.records[topic][partition] = newLegacyRecords(set)
}

//AddBatch adds a batch to produce request
func (p *ProduceRequest) AddBatch(topic string, partition int32, batch *RecordBatch) {
	p.ensureRecords(topic, partition)
	p.records[topic][partition] = newDefaultRecords(batch)
}
