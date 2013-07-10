package kafka

import "sync"

type brokerKey struct {
	topic     string
	partition int32
}

type brokerManager struct {
	defaultBroker *broker
	leaders       map[brokerKey]*broker
	leadersLock   sync.RWMutex
}

func newBrokerManager(host string, port int32) (bm *brokerManager, err error) {
	bm = new(brokerManager)
	bm.defaultBroker, err = newBroker(host, port)
	if err != nil {
		return nil, err
	}
	bm.leaders = make(map[brokerKey]*broker)
	return bm, nil
}

func (bm *brokerManager) lookupLeader(topic string, partition int32) *broker {
	bm.leadersLock.RLock()
	tmp := bm.leaders[brokerKey{topic, partition}]
	bm.leadersLock.RUnlock()
	return tmp
}

func (bm *brokerManager) refreshTopic(topic string) {
}
