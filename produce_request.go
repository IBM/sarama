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
	// WaitForAll waits for all replicas to commit before responding.
	WaitForAll RequiredAcks = -1
)

type ProduceRequest struct {
	RequiredAcks   RequiredAcks
	Timeout        int32
	Version        int16 // v1 requires Kafka 0.9, v2 requires Kafka 0.10
	msgSets        map[string]map[int32]*MessageSet
	metricRegistry metrics.Registry
}

func (p *ProduceRequest) encode(pe packetEncoder) error {
	pe.putInt16(int16(p.RequiredAcks))
	pe.putInt32(p.Timeout)
	err := pe.putArrayLength(len(p.msgSets))
	if err != nil {
		return err
	}
	doRecordMetrics := pe.doRecordMetrics() && p.metricRegistry != nil
	var batchSizeMetric metrics.Histogram
	var compressionRatioMetric metrics.Histogram
	if doRecordMetrics {
		batchSizeMetric = getOrRegisterHistogram("batch-size", p.metricRegistry)
		// Register the observer for the compression ratio
		compressionRatioMetric = getOrRegisterHistogram("compression-rate", p.metricRegistry)
	}

	totalRecordCount := int64(0)
	for topic, partitions := range p.msgSets {
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
		if doRecordMetrics {
			topicCompressionRatioMetric = getOrRegisterTopicHistogram("compression-rate", topic, p.metricRegistry)
		}
		for id, msgSet := range partitions {
			startOffset := pe.offset()
			pe.putInt32(id)
			pe.push(&lengthField{})
			err = msgSet.encode(pe)
			if err != nil {
				return err
			}
			err = pe.pop()
			if err != nil {
				return err
			}
			if doRecordMetrics {
				for _, messageBlock := range msgSet.Messages {
					// Default compression ratio for raw messages
					compressionRatio := 1.0
					// Is this a fake "message" wrapping real messages?
					if messageBlock.Msg.Set != nil {
						topicRecordCount += int64(len(messageBlock.Msg.Set.Messages))
						if len(messageBlock.Msg.Value) == 0 {
							// We should be in the real encoder pass with the compressedCache field filled
							compressionRatio = float64(len(messageBlock.Msg.compressedCache)) /
								float64(len(messageBlock.Msg.Value))
						}

					} else {
						topicRecordCount++
					}
					// Histogram do not support decimal values, let's use an integer percentage
					intCompressionRatio := int64(100 * compressionRatio)
					compressionRatioMetric.Update(intCompressionRatio)
					topicCompressionRatioMetric.Update(intCompressionRatio)
				}
				batchSize := int64(pe.offset() - startOffset)
				batchSizeMetric.Update(batchSize)
				getOrRegisterTopicHistogram("batch-size", topic, p.metricRegistry).Update(batchSize)
			}
		}
		if topicRecordCount > 0 {
			getOrRegisterTopicMeter("record-send-rate", topic, p.metricRegistry).Mark(topicRecordCount)
			getOrRegisterTopicHistogram("records-per-request", topic, p.metricRegistry).Update(topicRecordCount)
			totalRecordCount += topicRecordCount
		}
	}
	if totalRecordCount > 0 {
		metrics.GetOrRegisterMeter("record-send-rate", p.metricRegistry).Mark(totalRecordCount)
		getOrRegisterHistogram("records-per-request", p.metricRegistry).Update(totalRecordCount)
	}

	return nil
}

func (p *ProduceRequest) decode(pd packetDecoder, version int16) error {
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
	p.msgSets = make(map[string]map[int32]*MessageSet)
	for i := 0; i < topicCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		p.msgSets[topic] = make(map[int32]*MessageSet)
		for j := 0; j < partitionCount; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			messageSetSize, err := pd.getInt32()
			if err != nil {
				return err
			}
			msgSetDecoder, err := pd.getSubset(int(messageSetSize))
			if err != nil {
				return err
			}
			msgSet := &MessageSet{}
			err = msgSet.decode(msgSetDecoder)
			if err != nil {
				return err
			}
			p.msgSets[topic][partition] = msgSet
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
	default:
		return minVersion
	}
}

func (p *ProduceRequest) AddMessage(topic string, partition int32, msg *Message) {
	if p.msgSets == nil {
		p.msgSets = make(map[string]map[int32]*MessageSet)
	}

	if p.msgSets[topic] == nil {
		p.msgSets[topic] = make(map[int32]*MessageSet)
	}

	set := p.msgSets[topic][partition]

	if set == nil {
		set = new(MessageSet)
		p.msgSets[topic][partition] = set
	}

	set.addMessage(msg)
}

func (p *ProduceRequest) AddSet(topic string, partition int32, set *MessageSet) {
	if p.msgSets == nil {
		p.msgSets = make(map[string]map[int32]*MessageSet)
	}

	if p.msgSets[topic] == nil {
		p.msgSets[topic] = make(map[int32]*MessageSet)
	}

	p.msgSets[topic][partition] = set
}
