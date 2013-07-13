package kafka

import (
	"sort"
	"sync"
)

type metadataCache struct {
	client  *Client
	brokers map[int32]*broker          // maps broker ids to brokers
	leaders map[string]map[int32]int32 // maps topics to partition ids to broker ids
	lock    sync.RWMutex               // protects access to the maps, only one since they're always accessed together
}

func newMetadataCache(client *Client, host string, port int32) (*metadataCache, error) {
	mc := new(metadataCache)

	starter, err := newBroker(host, port)
	if err != nil {
		return nil, err
	}

	mc.client = client
	mc.brokers = make(map[int32]*broker)
	mc.leaders = make(map[string]map[int32]int32)

	mc.brokers[starter.id] = starter

	// do an initial fetch of all cluster metadata by specifing an empty list of topics
	err = mc.refreshTopics(make([]*string, 0))
	if err != nil {
		return nil, err
	}

	return mc, nil
}

func (mc *metadataCache) leader(topic string, partition_id int32) *broker {
	mc.lock.RLock()
	defer mc.lock.RUnlock()

	partitions := mc.leaders[topic]
	if partitions != nil {
		leader := partitions[partition_id]
		if leader == -1 {
			return nil
		} else {
			return mc.brokers[leader]
		}
	}

	return nil
}

func (mc *metadataCache) any() *broker {
	mc.lock.RLock()
	defer mc.lock.RUnlock()

	for _, broker := range mc.brokers {
		return broker
	}

	return nil
}

func (mc *metadataCache) partitions(topic string) []int32 {
	mc.lock.RLock()
	defer mc.lock.RUnlock()

	partitions := mc.leaders[topic]
	if partitions == nil {
		return nil
	}

	ret := make([]int32, len(partitions))
	for id, _ := range partitions {
		ret = append(ret, id)
	}

	sort.Sort(int32Slice(ret))
	return ret
}

func (mc *metadataCache) refreshTopics(topics []*string) error {
	broker := mc.any()
	if broker == nil {
		return OutOfBrokers{}
	}

	response := new(metadataResponse)
	err := broker.SendAndReceive(mc.client.id, &metadataRequest{topics}, response)
	if err != nil {
		return err
	}

	mc.lock.Lock()
	defer mc.lock.Unlock()

	for i := range response.brokers {
		broker := &response.brokers[i]
		mc.brokers[broker.id] = broker
	}

	for i := range response.topics {
		topic := &response.topics[i]
		if topic.err != NO_ERROR {
			return topic.err
		}
		mc.leaders[*topic.name] = make(map[int32]int32, len(topic.partitions))
		for j := range topic.partitions {
			partition := &topic.partitions[j]
			if partition.err != NO_ERROR {
				return partition.err
			}
			mc.leaders[*topic.name][partition.id] = partition.leader
		}
	}

	return nil
}

func (mc *metadataCache) refreshTopic(topic string) error {
	tmp := make([]*string, 1)
	tmp[0] = &topic
	return mc.refreshTopics(tmp)
}
