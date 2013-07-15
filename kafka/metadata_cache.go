package kafka

import k "sarama/protocol"

import (
	"sort"
	"sync"
)

type metadataCache struct {
	client  *Client
	brokers map[int32]*k.Broker        // maps broker ids to brokers
	leaders map[string]map[int32]int32 // maps topics to partition ids to broker ids
	lock    sync.RWMutex               // protects access to the maps, only one since they're always accessed together
}

func newMetadataCache(client *Client, host string, port int32) (*metadataCache, error) {
	mc := new(metadataCache)

	starter := k.NewBroker(host, port)
	err := starter.Connect()
	if err != nil {
		return nil, err
	}

	mc.client = client
	mc.brokers = make(map[int32]*k.Broker)
	mc.leaders = make(map[string]map[int32]int32)

	mc.brokers[starter.ID()] = starter

	// do an initial fetch of all cluster metadata by specifing an empty list of topics
	err = mc.refreshTopics(make([]*string, 0))
	if err != nil {
		return nil, err
	}

	return mc, nil
}

func (mc *metadataCache) removeBroker(broker *k.Broker) {
	if broker == nil {
		return
	}

	mc.lock.RLock()
	defer mc.lock.RUnlock()

	delete(mc.brokers, broker.ID())
	go broker.Close()
}

func (mc *metadataCache) leader(topic string, partition_id int32) *k.Broker {
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

func (mc *metadataCache) any() *k.Broker {
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

func (mc *metadataCache) update(data *k.MetadataResponse) error {
	// connect to the brokers before taking the lock, as this can take a while
	// to timeout if one of them isn't reachable
	for _, broker := range data.Brokers {
		err := broker.Connect()
		if err != nil {
			return err
		}
	}

	mc.lock.Lock()
	defer mc.lock.Unlock()

	for _, broker := range data.Brokers {
		if mc.brokers[broker.ID()] != nil {
			go mc.brokers[broker.ID()].Close()
		}
		mc.brokers[broker.ID()] = broker
	}

	for _, topic := range data.Topics {
		if topic.Err != k.NO_ERROR {
			return topic.Err
		}
		mc.leaders[*topic.Name] = make(map[int32]int32, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			if partition.Err != k.NO_ERROR {
				return partition.Err
			}
			mc.leaders[*topic.Name][partition.Id] = partition.Leader
		}
	}

	return nil
}

func (mc *metadataCache) refreshTopics(topics []*string) error {
	for broker := mc.any(); broker != nil; broker = mc.any() {
		response, err := broker.GetMetadata(mc.client.id, &k.MetadataRequest{Topics: topics})

		switch err.(type) {
		case nil:
			// valid response, use it
			return mc.update(response)
		case k.EncodingError:
			// didn't even send, return the error
			return err
		}

		// some other error, remove that broker and try again
		mc.removeBroker(broker)

	}

	return OutOfBrokers
}

func (mc *metadataCache) refreshTopic(topic string) error {
	tmp := make([]*string, 1)
	tmp[0] = &topic
	return mc.refreshTopics(tmp)
}
