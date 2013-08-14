package sarama

import (
	"sort"
	"sync"
	"time"
)

// Client is a generic Kafka client. It manages connections to one or more Kafka brokers.
// You MUST call Close() on a client to avoid leaks, it will not be garbage-collected
// automatically when it passes out of scope. A single client can be safely shared by
// multiple concurrent Producers and Consumers.
type Client struct {
	id      string                     // client id for broker requests
	brokers map[int32]*Broker          // maps broker ids to brokers
	leaders map[string]map[int32]int32 // maps topics to partition ids to broker ids
	lock    sync.RWMutex               // protects access to the maps, only one since they're always written together
}

// NewClient creates a new Client with the given client ID. It connects to the broker at the given
// host:port address, and uses that broker to automatically fetch metadata on the rest of the kafka cluster.
// If metadata cannot be retrieved (even if the connection otherwise succeeds) then the client is not created.
func NewClient(id string, host string, port int32) (client *Client, err error) {
	tmp := NewBroker(host, port)
	err = tmp.Open()
	if err != nil {
		return nil, err
	}
	_, err = tmp.Connected()
	if err != nil {
		return nil, err
	}

	client = new(Client)
	client.id = id

	client.brokers = make(map[int32]*Broker)
	client.leaders = make(map[string]map[int32]int32)

	// add it to the set so that refreshTopics can find it
	// brokers created through NewBroker() have an ID of -1, which won't conflict with
	// whatever the metadata request returns
	client.brokers[tmp.ID()] = tmp

	// do an initial fetch of all cluster metadata by specifing an empty list of topics
	err = client.refreshTopics(make([]string, 0), 3)
	if err != nil {
		client.Close() // this closes tmp, since it's still in the brokers hash
		return nil, err
	}

	// So apparently a kafka broker is not required to return its own address in response
	// to a 'give me *all* the metadata request'... I'm not sure if that's because you're
	// assumed to have it already or what. Regardless, this means that we can't assume we can
	// disconnect our tmp broker here, since if it didn't return itself to us we want to keep
	// it around anyways. The worst that happens is we end up with two connections to the same
	// broker, one with ID -1 and one with the real ID.

	return client, nil
}

// Close shuts down all broker connections managed by this client. It is required to call this function before
// a client object passes out of scope, as it will otherwise leak memory. You must close any Producers or Consumers
// using a client before you close the client.
func (client *Client) Close() {
	client.lock.Lock()
	defer client.lock.Unlock()

	for _, broker := range client.brokers {
		go broker.Close()
	}
	client.brokers = nil
	client.leaders = nil
}

// functions for use by producers and consumers
// if Go had the concept they would be marked 'protected'

func (client *Client) leader(topic string, partition_id int32) (*Broker, error) {
	leader := client.cachedLeader(topic, partition_id)

	if leader == nil {
		err := client.refreshTopic(topic)
		if err != nil {
			return nil, err
		}
		leader = client.cachedLeader(topic, partition_id)
	}

	if leader == nil {
		return nil, UNKNOWN_TOPIC_OR_PARTITION
	}

	return leader, nil
}

func (client *Client) partitions(topic string) ([]int32, error) {
	partitions := client.cachedPartitions(topic)

	if partitions == nil {
		err := client.refreshTopic(topic)
		if err != nil {
			return nil, err
		}
		partitions = client.cachedPartitions(topic)
	}

	if partitions == nil {
		return nil, NoSuchTopic
	}

	return partitions, nil
}

func (client *Client) disconnectBroker(broker *Broker) {
	client.lock.Lock()
	defer client.lock.Unlock()

	// we don't need to update the leaders hash, it will automatically get refreshed next time because
	// the broker lookup will return nil
	delete(client.brokers, broker.ID())
	go broker.Close()
}

func (client *Client) refreshTopic(topic string) error {
	tmp := make([]string, 1)
	tmp[0] = topic
	// we permit three retries by default, 'cause that seemed like a nice number
	return client.refreshTopics(tmp, 3)
}

// truly private helper functions

func (client *Client) refreshTopics(topics []string, retries int) error {
	for broker := client.any(); broker != nil; broker = client.any() {
		response, err := broker.GetMetadata(client.id, &MetadataRequest{Topics: topics})

		switch err {
		case nil:
			// valid response, use it
			retry, err := client.update(response)
			switch {
			case err != nil:
				return err
			case len(retry) == 0:
				return nil
			default:
				if retries <= 0 {
					return LEADER_NOT_AVAILABLE
				}
				time.Sleep(250 * time.Millisecond) // wait for leader election
				return client.refreshTopics(retry, retries-1)
			}
		case EncodingError:
			// didn't even send, return the error
			return err
		}

		// some other error, remove that broker and try again
		client.disconnectBroker(broker)
	}

	return OutOfBrokers
}

func (client *Client) any() *Broker {
	client.lock.RLock()
	defer client.lock.RUnlock()

	for _, broker := range client.brokers {
		return broker
	}

	return nil
}

func (client *Client) cachedLeader(topic string, partition_id int32) *Broker {
	client.lock.RLock()
	defer client.lock.RUnlock()

	partitions := client.leaders[topic]
	if partitions != nil {
		leader, ok := partitions[partition_id]
		if ok {
			return client.brokers[leader]
		}
	}

	return nil
}

func (client *Client) cachedPartitions(topic string) []int32 {
	client.lock.RLock()
	defer client.lock.RUnlock()

	partitions := client.leaders[topic]
	if partitions == nil {
		return nil
	}

	ret := make([]int32, 0, len(partitions))
	for id, _ := range partitions {
		ret = append(ret, id)
	}

	sort.Sort(int32Slice(ret))
	return ret
}

// if no fatal error, returns a list of topics that need retrying due to LEADER_NOT_AVAILABLE
func (client *Client) update(data *MetadataResponse) ([]string, error) {
	client.lock.Lock()
	defer client.lock.Unlock()

	// First discard brokers that we already know about. This avoids bouncing TCP connections,
	// and especially avoids closing valid connections out from under other code which may be trying
	// to use them.
	var newBrokers []*Broker
	for _, broker := range data.Brokers {
		if !broker.Equals(client.brokers[broker.ID()]) {
			newBrokers = append(newBrokers, broker)
		}
	}

	// Now asynchronously try to open connections to the new brokers. We don't care if they
	// fail, since maybe that broker is unreachable but doesn't have a topic we care about.
	// If it fails and we do care, whoever tries to use it will get the connection error.
	// If we have an old broker with that ID (but a different host/port, since they didn't
	// compare as equals above) then close and remove that broker before saving the new one.
	for _, broker := range newBrokers {
		if client.brokers[broker.ID()] != nil {
			go client.brokers[broker.ID()].Close()
		}
		broker.Open()
		client.brokers[broker.ID()] = broker
	}

	toRetry := make(map[string]bool)

	for _, topic := range data.Topics {
		switch topic.Err {
		case NO_ERROR:
			break
		case LEADER_NOT_AVAILABLE:
			toRetry[topic.Name] = true
		default:
			return nil, topic.Err
		}
		client.leaders[topic.Name] = make(map[int32]int32, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			switch partition.Err {
			case LEADER_NOT_AVAILABLE:
				toRetry[topic.Name] = true
				delete(client.leaders[topic.Name], partition.Id)
			case NO_ERROR:
				client.leaders[topic.Name][partition.Id] = partition.Leader
			default:
				return nil, partition.Err
			}
		}
	}

	ret := make([]string, 0, len(toRetry))
	for topic, _ := range toRetry {
		ret = append(ret, topic)
	}
	return ret, nil
}
