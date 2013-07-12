package kafka

type fetchResponsePartitionBlock struct {
	id                  int32
	err                 KError
	highWaterMarkOffset int64
	msgSet              messageSet
}

func (pr *fetchResponsePartitionBlock) decode(pd packetDecoder) (err error) {
	pr.id, err = pd.getInt32()
	if err != nil {
		return err
	}

	pr.err, err = pd.getError()
	if err != nil {
		return err
	}

	pr.highWaterMarkOffset, err = pd.getInt64()
	if err != nil {
		return err
	}

	return nil
}

type fetchResponseTopicBlock struct {
	name       *string
	partitions []fetchResponsePartitionBlock
}

func (pr *fetchResponseTopicBlock) decode(pd packetDecoder) (err error) {
	pr.name, err = pd.getString()
	if err != nil {
		return err
	}

	n, err := pd.getArrayCount()
	if err != nil {
		return err
	}

	pr.partitions = make([]fetchResponsePartitionBlock, n)
	for i := range pr.partitions {
		err = (&pr.partitions[i]).decode(pd)
		if err != nil {
			return err
		}
	}

	return nil
}

type fetchResponse struct {
	topics []fetchResponseTopicBlock
}

func (pr *fetchResponse) decode(pd packetDecoder) (err error) {
	n, err := pd.getArrayCount()
	if err != nil {
		return err
	}

	pr.topics = make([]fetchResponseTopicBlock, n)
	for i := range pr.topics {
		err = (&pr.topics[i]).decode(pd)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pr *fetchResponse) staleTopics() []*string {
	ret := make([]*string, 0)

	for i := range pr.topics {
		topic := &pr.topics[i]

	currentTopic:
		for j := range topic.partitions {
			partition := &topic.partitions[j]
			switch partition.err {
			case UNKNOWN, UNKNOWN_TOPIC_OR_PARTITION, LEADER_NOT_AVAILABLE, NOT_LEADER_FOR_PARTITION:
				ret = append(ret, topic.name)
				break currentTopic
			}
		}
	}

	return ret
}
