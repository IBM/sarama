package kafka

import "sync"

type topicPartition struct {
	topic     string
	partition int32
}

type brokerManager struct {
	client        *Client
	defaultBroker *broker

	brokers     map[int32]*broker
	leaders     map[topicPartition]int32
	brokersLock sync.RWMutex
}

func newBrokerManager(client *Client, host string, port int32) (bm *brokerManager, err error) {
	bm = new(brokerManager)

	bm.client = client

	// we create a new broker object as the default 'master' broker
	// if this broker is also a leader then we will end up with two broker objects for it, but that's not a big deal
	bm.defaultBroker, err = newBroker(host, port)
	if err != nil {
		return nil, err
	}

	bm.brokers = make(map[int32]*broker)
	bm.leaders = make(map[topicPartition]int32)

	// do an initial fetch of all cluster metadata by specifing an empty list of topics
	err = bm.refreshTopics(make([]*string, 0))
	if err != nil {
		return nil, err
	}

	return bm, nil
}

func (bm *brokerManager) getLeader(topic string, partition int32) (*broker, error) {
	var broker *broker = nil
	bm.brokersLock.RLock()
	id, ok := bm.leaders[topicPartition{topic, partition}]
	if ok {
		broker = bm.brokers[id]
	}
	bm.brokersLock.RUnlock()

	if broker == nil {
		err := bm.refreshTopic(topic)
		if err != nil {
			return nil, err
		}
		bm.brokersLock.RLock()
		broker = bm.brokers[bm.leaders[topicPartition{topic, partition}]]
		bm.brokersLock.RUnlock()
	}

	if broker == nil {
		return nil, UNKNOWN_TOPIC_OR_PARTITION
	}

	return broker, nil
}

func (bm *brokerManager) tryLeader(topic string, partition int32, req encoder, res decoder) error {
	b, err := bm.getLeader(topic, partition)
	if err != nil {
		return err
	}

	responseChan, err := b.sendRequest(bm.client.id, req)
	if err != nil {
		// errors that would make us refresh the broker metadata don't get returned here,
		// they'd come through responseChan.errors, so it's safe to just return here
		return err
	}

	select {
	case buf := <-responseChan.packets:
		decoder := realDecoder{raw: buf}
		err = res.decode(&decoder)
	case err = <-responseChan.errors:
	}

	if err == nil {
		// successfully received and decoded the packet, we're done
		return nil
	}

	// we got an error, so discard that broker
	bm.brokersLock.Lock()
	delete(bm.brokers, b.id)
	bm.brokersLock.Unlock()

	// then do the whole thing again
	// (the metadata for the broker gets refreshed automatically in getLeader)
	// if we get a broker here, it's guaranteed to be fresh, so if it fails then
	// we pass that error back to the user (as opposed to retrying indefinitely)
	b, err = bm.getLeader(topic, partition)
	if err != nil {
		return err
	}

	responseChan, err = b.sendRequest(bm.client.id, req)
	if err != nil {
		return err
	}

	select {
	case buf := <-responseChan.packets:
		decoder := realDecoder{raw: buf}
		err = res.decode(&decoder)
		return err
	case err = <-responseChan.errors:
		return err
	}

}

func (bm *brokerManager) getDefault() *broker {

	if bm.defaultBroker == nil {
		bm.brokersLock.RLock()
		defer bm.brokersLock.RUnlock()
		for _, id := range bm.leaders {
			bm.defaultBroker = bm.brokers[id]
			break
		}
	}

	return bm.defaultBroker
}

func (bm *brokerManager) tryDefaultBrokers(req encoder, res decoder) error {
	for b := bm.getDefault(); b != nil; b = bm.getDefault() {
		responseChan, err := b.sendRequest(bm.client.id, req)
		if err != nil {
			return err
		}

		select {
		case buf := <-responseChan.packets:
			decoder := realDecoder{raw: buf}
			err = res.decode(&decoder)
			return err
		case <-responseChan.errors:
			bm.defaultBroker = nil
			bm.brokersLock.Lock()
			delete(bm.brokers, b.id)
			bm.brokersLock.Unlock()
		}
	}
	return OutOfBrokers{}
}

func (bm *brokerManager) refreshTopics(topics []*string) error {
	response := new(metadata)
	err := bm.tryDefaultBrokers(&metadataRequest{topics}, response)
	if err != nil {
		return err
	}

	bm.brokersLock.Lock()
	defer bm.brokersLock.Unlock()

	for i := range response.brokers {
		broker := &response.brokers[i]
		bm.brokers[broker.id] = broker
	}

	for i := range response.topics {
		topic := &response.topics[i]
		if topic.err != NO_ERROR {
			return topic.err
		}
		for j := range topic.partitions {
			partition := &topic.partitions[j]
			if partition.err != NO_ERROR {
				return partition.err
			}
			bm.leaders[topicPartition{*topic.name, partition.id}] = partition.leader
		}
	}

	return nil
}

func (bm *brokerManager) refreshTopic(topic string) error {
	tmp := make([]*string, 1)
	tmp[0] = &topic
	return bm.refreshTopics(tmp)
}
