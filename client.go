package sarama

import (
	"sort"
	"sync"
	"time"
)

// ClientConfig is used to pass multiple configuration options to NewClient.
type ClientConfig struct {
	MetadataRetries            int           // How many times to retry a metadata request when a partition is in the middle of leader election.
	WaitForElection            time.Duration // How long to wait for leader election to finish between retries.
	DefaultBrokerConf          *BrokerConfig // Default configuration for broker connections created by this client.
	BackgroundRefreshFrequency time.Duration // How frequently the client will refresh the cluster metadata in the background. Defaults to 10 minutes. Set to 0 to disable.
}

// Client is a generic Kafka client. It manages connections to one or more Kafka brokers.
// You MUST call Close() on a client to avoid leaks, it will not be garbage-collected
// automatically when it passes out of scope. A single client can be safely shared by
// multiple concurrent Producers and Consumers.
type Client struct {
	id     string
	config ClientConfig
	closer chan struct{}

	// the broker addresses given to us through the constructor are not guaranteed to be returned in
	// the cluster metadata (I *think* it only returns brokers who are currently leading partitions?)
	// so we store them separately
	seedBrokerAddrs []string
	seedBroker      *Broker
	deadBrokerAddrs map[string]struct{}

	brokers  map[int32]*Broker                       // maps broker ids to brokers
	metadata map[string]map[int32]*PartitionMetadata // maps topics to partition ids to metadata
	lock     sync.RWMutex                            // protects access to the maps, only one since they're always written together
}

// NewClient creates a new Client with the given client ID. It connects to one of the given broker addresses
// and uses that broker to automatically fetch metadata on the rest of the kafka cluster. If metadata cannot
// be retrieved from any of the given broker addresses, the client is not created.
func NewClient(id string, addrs []string, config *ClientConfig) (*Client, error) {
	Logger.Println("Initializing new client")

	if config == nil {
		config = NewClientConfig()
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if len(addrs) < 1 {
		return nil, ConfigurationError("You must provide at least one broker address")
	}

	client := &Client{
		id:              id,
		config:          *config,
		closer:          make(chan struct{}),
		seedBrokerAddrs: addrs,
		seedBroker:      NewBroker(addrs[0]),
		deadBrokerAddrs: make(map[string]struct{}),
		brokers:         make(map[int32]*Broker),
		metadata:        make(map[string]map[int32]*PartitionMetadata),
	}
	_ = client.seedBroker.Open(config.DefaultBrokerConf)

	// do an initial fetch of all cluster metadata by specifing an empty list of topics
	err := client.RefreshAllMetadata()
	switch err {
	case nil:
		break
	case LeaderNotAvailable:
		// indicates that maybe part of the cluster is down, but is not fatal to creating the client
		Logger.Println(err)
	default:
		_ = client.Close()
		return nil, err
	}
	go withRecover(client.backgroundMetadataUpdater)

	Logger.Println("Successfully initialized new client")

	return client, nil
}

// Close shuts down all broker connections managed by this client. It is required to call this function before
// a client object passes out of scope, as it will otherwise leak memory. You must close any Producers or Consumers
// using a client before you close the client.
func (client *Client) Close() error {
	// Check to see whether the client is closed
	if client.Closed() {
		// Chances are this is being called from a defer() and the error will go unobserved
		// so we go ahead and log the event in this case.
		Logger.Printf("Close() called on already closed client")
		return ClosedClient
	}

	client.lock.Lock()
	defer client.lock.Unlock()
	Logger.Println("Closing Client")

	for _, broker := range client.brokers {
		safeAsyncClose(broker)
	}
	client.brokers = nil
	client.metadata = nil

	if client.seedBroker != nil {
		safeAsyncClose(client.seedBroker)
	}

	close(client.closer)

	return nil
}

// Partitions returns the sorted list of available partition IDs for the given topic.
func (client *Client) Partitions(topic string) ([]int32, error) {
	// Check to see whether the client is closed
	if client.Closed() {
		return nil, ClosedClient
	}

	partitions := client.cachedPartitions(topic)

	// len==0 catches when it's nil (no such topic) and the odd case when every single
	// partition is undergoing leader election simultaneously. Callers have to be able to handle
	// this function returning an empty slice (which is a valid return value) but catching it
	// here the first time (note we *don't* catch it below where we return NoSuchTopic) triggers
	// a metadata refresh as a nicety so callers can just try again and don't have to manually
	// trigger a refresh (otherwise they'd just keep getting a stale cached copy).
	if len(partitions) == 0 {
		err := client.RefreshTopicMetadata(topic)
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

// Topics returns the set of available topics as retrieved from the cluster metadata.
func (client *Client) Topics() ([]string, error) {
	// Check to see whether the client is closed
	if client.Closed() {
		return nil, ClosedClient
	}

	client.lock.RLock()
	defer client.lock.RUnlock()

	ret := make([]string, 0, len(client.metadata))
	for topic := range client.metadata {
		ret = append(ret, topic)
	}

	return ret, nil
}

func (client *Client) getMetadata(topic string, partitionID int32) (*PartitionMetadata, error) {
	metadata := client.cachedMetadata(topic, partitionID)

	if metadata == nil {
		err := client.RefreshTopicMetadata(topic)
		if err != nil {
			return nil, err
		}
		metadata = client.cachedMetadata(topic, partitionID)
	}

	if metadata == nil {
		return nil, UnknownTopicOrPartition
	}

	return metadata, nil
}

func (client *Client) Replicas(topic string, partitionID int32) ([]int32, error) {
	metadata, err := client.getMetadata(topic, partitionID)

	if err != nil {
		return nil, err
	}

	return dupeAndSort(metadata.Replicas), nil
}

func (client *Client) ReplicasInSync(topic string, partitionID int32) ([]int32, error) {
	metadata, err := client.getMetadata(topic, partitionID)

	if err != nil {
		return nil, err
	}

	return dupeAndSort(metadata.Isr), nil
}

// Leader returns the broker object that is the leader of the current topic/partition, as
// determined by querying the cluster metadata.
func (client *Client) Leader(topic string, partitionID int32) (*Broker, error) {
	leader := client.cachedLeader(topic, partitionID)

	if leader == nil {
		err := client.RefreshTopicMetadata(topic)
		if err != nil {
			return nil, err
		}
		leader = client.cachedLeader(topic, partitionID)
	}

	if leader == nil {
		return nil, UnknownTopicOrPartition
	}

	return leader, nil
}

// RefreshTopicMetadata takes a list of topics and queries the cluster to refresh the
// available metadata for those topics.
func (client *Client) RefreshTopicMetadata(topics ...string) error {
	return client.refreshMetadata(topics, client.config.MetadataRetries)
}

// RefreshAllMetadata queries the cluster to refresh the available metadata for all topics.
func (client *Client) RefreshAllMetadata() error {
	// Kafka refreshes all when you encode it an empty array...
	return client.refreshMetadata(make([]string, 0), client.config.MetadataRetries)
}

// GetOffset queries the cluster to get the most recent available offset at the given
// time on the topic/partition combination.
func (client *Client) GetOffset(topic string, partitionID int32, where OffsetTime) (int64, error) {
	broker, err := client.Leader(topic, partitionID)
	if err != nil {
		return -1, err
	}

	request := &OffsetRequest{}
	request.AddBlock(topic, partitionID, where, 1)

	response, err := broker.GetAvailableOffsets(client.id, request)
	if err != nil {
		return -1, err
	}

	block := response.GetBlock(topic, partitionID)
	if block == nil {
		return -1, IncompleteResponse
	}
	if block.Err != NoError {
		return -1, block.Err
	}
	if len(block.Offsets) != 1 {
		return -1, OffsetOutOfRange
	}

	return block.Offsets[0], nil
}

// misc private helper functions

// XXX: see https://github.com/Shopify/sarama/issues/15
//      and https://github.com/Shopify/sarama/issues/23
// disconnectBroker is a bad hacky way to accomplish broker management. It should be replaced with
// something sane and the replacement should be made part of the public Client API
func (client *Client) disconnectBroker(broker *Broker) {
	client.lock.Lock()
	defer client.lock.Unlock()
	Logger.Printf("Disconnecting Broker %d\n", broker.ID())

	client.deadBrokerAddrs[broker.addr] = struct{}{}

	if broker == client.seedBroker {
		client.seedBrokerAddrs = client.seedBrokerAddrs[1:]
		if len(client.seedBrokerAddrs) > 0 {
			client.seedBroker = NewBroker(client.seedBrokerAddrs[0])
			_ = client.seedBroker.Open(client.config.DefaultBrokerConf)
		} else {
			client.seedBroker = nil
		}
	} else {
		// we don't need to update the leaders hash, it will automatically get refreshed next time because
		// the broker lookup will return nil
		delete(client.brokers, broker.ID())
	}

	safeAsyncClose(broker)
}

func (client *Client) Closed() bool {
	return client.brokers == nil
}

func (client *Client) refreshMetadata(topics []string, retries int) error {
	// This function is a sort of central point for most functions that create new
	// resources.  Check to see if we're dealing with a closed Client and error
	// out immediately if so.
	if client.Closed() {
		return ClosedClient
	}

	// Kafka will throw exceptions on an empty topic and not return a proper
	// error. This handles the case by returning an error instead of sending it
	// off to Kafka. See: https://github.com/Shopify/sarama/pull/38#issuecomment-26362310
	for _, topic := range topics {
		if len(topic) == 0 {
			return NoSuchTopic
		}
	}

	for broker := client.any(); broker != nil; broker = client.any() {
		Logger.Printf("Fetching metadata from broker %s\n", broker.addr)
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
					return LeaderNotAvailable
				}
				Logger.Printf("Failed to fetch metadata from broker %s, waiting %dms... (%d retries remaining)\n", broker.addr, client.config.WaitForElection/time.Millisecond, retries)
				time.Sleep(client.config.WaitForElection) // wait for leader election
				return client.refreshMetadata(retry, retries-1)
			}
		case EncodingError:
			// didn't even send, return the error
			return err
		}

		// some other error, remove that broker and try again
		Logger.Println("Unexpected error from GetMetadata, closing broker:", err)
		client.disconnectBroker(broker)
	}

	if retries > 0 {
		Logger.Printf("Out of available brokers. Resurrecting dead brokers after %dms... (%d retries remaining)\n", client.config.WaitForElection/time.Millisecond, retries)
		time.Sleep(client.config.WaitForElection)
		client.resurrectDeadBrokers()
		return client.refreshMetadata(topics, retries-1)
	} else {
		Logger.Printf("Out of available brokers.\n")
	}

	return OutOfBrokers
}

func (client *Client) resurrectDeadBrokers() {
	client.lock.Lock()
	defer client.lock.Unlock()

	for _, addr := range client.seedBrokerAddrs {
		client.deadBrokerAddrs[addr] = struct{}{}
	}

	client.seedBrokerAddrs = []string{}
	for addr := range client.deadBrokerAddrs {
		client.seedBrokerAddrs = append(client.seedBrokerAddrs, addr)
	}
	client.deadBrokerAddrs = make(map[string]struct{})

	client.seedBroker = NewBroker(client.seedBrokerAddrs[0])
	_ = client.seedBroker.Open(client.config.DefaultBrokerConf)
}

func (client *Client) any() *Broker {
	client.lock.RLock()
	defer client.lock.RUnlock()

	for _, broker := range client.brokers {
		return broker
	}

	return client.seedBroker
}

func (client *Client) cachedLeader(topic string, partitionID int32) *Broker {
	client.lock.RLock()
	defer client.lock.RUnlock()

	partitions := client.metadata[topic]
	if partitions != nil {
		metadata, ok := partitions[partitionID]
		if ok {
			return client.brokers[metadata.Leader]
		}
	}

	return nil
}

func (client *Client) cachedMetadata(topic string, partitionID int32) *PartitionMetadata {
	client.lock.RLock()
	defer client.lock.RUnlock()

	partitions := client.metadata[topic]
	if partitions != nil {
		return partitions[partitionID]
	}

	return nil
}

func (client *Client) cachedPartitions(topic string) []int32 {
	client.lock.RLock()
	defer client.lock.RUnlock()

	partitions := client.metadata[topic]
	if partitions == nil {
		return nil
	}

	ret := make([]int32, 0, len(partitions))
	for id := range partitions {
		ret = append(ret, id)
	}

	sort.Sort(int32Slice(ret))
	return ret
}

func (client *Client) backgroundMetadataUpdater() {
	if client.config.BackgroundRefreshFrequency == time.Duration(0) {
		return
	}

	ticker := time.NewTicker(client.config.BackgroundRefreshFrequency)
	for {
		select {
		case <-ticker.C:
			if err := client.RefreshAllMetadata(); err != nil {
				Logger.Println("Client background metadata update:", err)
			}
		case <-client.closer:
			ticker.Stop()
			return
		}
	}
}

// if no fatal error, returns a list of topics that need retrying due to LeaderNotAvailable
func (client *Client) update(data *MetadataResponse) ([]string, error) {
	client.lock.Lock()
	defer client.lock.Unlock()

	// For all the brokers we received:
	// - if it is a new ID, save it
	// - if it is an existing ID, but the address we have is stale, discard the old one and save it
	// - otherwise ignore it, replacing our existing one would just bounce the connection
	// We asynchronously try to open connections to the new brokers. We don't care if they
	// fail, since maybe that broker is unreachable but doesn't have a topic we care about.
	// If it fails and we do care, whoever tries to use it will get the connection error.
	for _, broker := range data.Brokers {
		if client.brokers[broker.ID()] == nil {
			_ = broker.Open(client.config.DefaultBrokerConf)
			client.brokers[broker.ID()] = broker
			Logger.Printf("Registered new broker #%d at %s", broker.ID(), broker.Addr())
		} else if broker.Addr() != client.brokers[broker.ID()].Addr() {
			safeAsyncClose(client.brokers[broker.ID()])
			_ = broker.Open(client.config.DefaultBrokerConf)
			client.brokers[broker.ID()] = broker
			Logger.Printf("Replaced registered broker #%d with %s", broker.ID(), broker.Addr())
		}
	}

	toRetry := make(map[string]bool)

	for _, topic := range data.Topics {
		switch topic.Err {
		case NoError:
			break
		case LeaderNotAvailable:
			toRetry[topic.Name] = true
		default:
			return nil, topic.Err
		}
		client.metadata[topic.Name] = make(map[int32]*PartitionMetadata, len(topic.Partitions))
		for _, partition := range topic.Partitions {
			switch partition.Err {
			case LeaderNotAvailable:
				toRetry[topic.Name] = true
				delete(client.metadata[topic.Name], partition.ID)
			case NoError:
				broker := client.brokers[partition.Leader]
				if _, present := client.deadBrokerAddrs[broker.Addr()]; present {
					if connected, _ := broker.Connected(); !connected {
						toRetry[topic.Name] = true
						delete(client.metadata[topic.Name], partition.ID)
						continue
					}
				}
				client.metadata[topic.Name][partition.ID] = partition
			default:
				return nil, partition.Err
			}
		}
	}

	ret := make([]string, 0, len(toRetry))
	for topic := range toRetry {
		ret = append(ret, topic)
	}
	return ret, nil
}

// NewClientConfig creates a new ClientConfig instance with sensible defaults
func NewClientConfig() *ClientConfig {
	return &ClientConfig{
		MetadataRetries:            3,
		WaitForElection:            250 * time.Millisecond,
		BackgroundRefreshFrequency: 10 * time.Minute,
	}
}

// Validate checks a ClientConfig instance. This will return a
// ConfigurationError if the specified values don't make sense.
func (config *ClientConfig) Validate() error {
	if config.MetadataRetries <= 0 {
		return ConfigurationError("Invalid MetadataRetries. Try 10")
	}

	if config.WaitForElection <= time.Duration(0) {
		return ConfigurationError("Invalid WaitForElection. Try 250*time.Millisecond")
	}

	if config.DefaultBrokerConf != nil {
		if err := config.DefaultBrokerConf.Validate(); err != nil {
			return err
		}
	}

	if config.BackgroundRefreshFrequency < time.Duration(0) {
		return ConfigurationError("Invalid BackgroundRefreshFrequency.")
	}

	return nil
}
