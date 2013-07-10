package kafka

import "sync"

type brokerKey struct {
	topic     string
	partition int32
}

type brokerManager struct {
	client        *Client
	defaultBroker *broker
	leaders       map[brokerKey]*broker
	leadersLock   sync.RWMutex
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

	bm.leaders = make(map[brokerKey]*broker)

	// do an initial fetch of all cluster metadata by specifing an empty list of topics
	err = bm.refreshTopics(make([]*string, 0))
	if err != nil {
		return nil, err
	}

	return bm, nil
}

func (bm *brokerManager) lookupLeader(topic string, partition int32) *broker {
	bm.leadersLock.RLock()
	defer bm.leadersLock.RUnlock()
	return bm.leaders[brokerKey{topic, partition}]
}

func (bm *brokerManager) getDefault() *broker {

	if bm.defaultBroker == nil {
		bm.leadersLock.RLock()
		defer bm.leadersLock.RUnlock()
		for _, bm.defaultBroker = range bm.leaders {
			break
		}
	}

	return bm.defaultBroker
}

func (bm *brokerManager) refreshTopics(topics []*string) error {
	b := bm.getDefault()
	if b == nil {
		return OutOfBrokers{}
	}

	responseChan, err := b.sendRequest(bm.client.id, &metadataRequest{topics})
	if err != nil {
		return err
	}

	decoder := realDecoder{raw: <-responseChan}
	response := new(metadata)
	err = response.decode(&decoder)
	if err != nil {
		return err
	}

	bm.leadersLock.Lock()
	defer bm.leadersLock.Unlock()

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
			bm.leaders[brokerKey{*topic.name, partition.id}] = response.brokerById(partition.leader)
		}
	}

	return nil
}

func (bm *brokerManager) refreshTopic(topic string) error {
	tmp := make([]*string, 1)
	tmp[0] = &topic
	return bm.refreshTopics(tmp)
}
