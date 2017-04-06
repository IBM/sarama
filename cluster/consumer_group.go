package cluster

import (
	"errors"
	"math"
	"sort"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	DiscardCommit = errors.New("sarama: commit discarded")
	NoCheckout    = errors.New("sarama: not checkout")
)

// A ConsumerGroup operates on all partitions of a single topic. The goal is to ensure
// each topic message is consumed only once, no matter of the number of consumer instances within
// a cluster, as described in: http://kafka.apache.org/documentation.html#distributionimpl.
//
// The ConsumerGroup internally creates multiple Consumer instances. It uses Zookkeper
// and follows a simple consumer rebalancing algorithm which allows all the consumers
// in a group to come into consensus on which consumer is consuming which partitions. Each
// ConsumerGroup can 'claim' 0-n partitions and will consume their messages until another
// ConsumerGroup instance with the same name joins or leaves the cluster.
//
// Unlike stated in the Kafka documentation, consumer rebalancing is *only* triggered on each
// addition or removal of consumers within the same group, while the addition of broker nodes
// and/or partition *does currently not trigger* a rebalancing cycle.
type ConsumerGroup struct {
	id, name, topic string

	config *sarama.ConsumerConfig
	client *sarama.Client
	zoo    *ZK
	claims []PartitionConsumer

	zkchange <-chan zk.Event
	claimed  chan *PartitionConsumer
	logger   Loggable

	checkout, force, stopper, done chan bool
}

// NewConsumerGroup creates a new consumer group for a given topic.
//
// You MUST call Close() on a consumer to avoid leaks, it will not be garbage-collected automatically when
// it passes out of scope (this is in addition to calling Close on the underlying client, which is still necessary).
func NewConsumerGroup(client *sarama.Client, zoo *ZK, name string, topic string, logger Loggable, config *sarama.ConsumerConfig) (group *ConsumerGroup, err error) {
	if config == nil {
		config = new(sarama.ConsumerConfig)
	}

	// Validate configuration
	if err = validateConsumerConfig(config); err != nil {
		return
	} else if topic == "" {
		return nil, sarama.ConfigurationError("Empty topic")
	} else if name == "" {
		return nil, sarama.ConfigurationError("Empty name")
	}

	// Register consumer group
	if err = zoo.RegisterGroup(name); err != nil {
		return
	}

	// Init struct
	group = &ConsumerGroup{
		id:    GUID.New(name),
		name:  name,
		topic: topic,

		config: config,
		client: client,
		zoo:    zoo,
		claims: make([]PartitionConsumer, 0),
		logger: logger,

		stopper:  make(chan bool),
		done:     make(chan bool),
		checkout: make(chan bool),
		force:    make(chan bool),
		claimed:  make(chan *PartitionConsumer),
	}

	// Register itself with zookeeper
	if err = zoo.RegisterConsumer(group.name, group.id, group.topic); err != nil {
		return nil, err
	}

	go group.signalLoop()
	return group, nil
}

// Checkout applies a callback function to a single partition consumer.
// The latest consumer offset is automatically comitted to zookeeper if successful.
// The callback may return a DiscardCommit error to skip the commit silently.
// Returns an error if any, but may also return a NoCheckout error to indicate
// that no partition was available. You should add an artificial delay keep your CPU cool.
func (cg *ConsumerGroup) Checkout(callback func(*PartitionConsumer) error) error {
	cg.checkout <- true
	claimed := <-cg.claimed

	if claimed == nil {
		return NoCheckout
	}

	err := callback(claimed)
	if err == DiscardCommit {
		err = nil
	} else if err == nil && claimed.offset > 0 {
		err = cg.Commit(claimed.partition, claimed.offset+1)
	}
	return err
}

// Process retrieves a bulk of events and applies a callback.
// The latest consumer offset is automatically comitted to zookeeper if successful.
// The callback may return a DiscardCommit error to skip the commit silently.
// Returns an error if any, but may also return a NoCheckout error to indicate
// that no partition was available. You should add an artificial delay keep your CPU cool.
func (cg *ConsumerGroup) Process(callback func(*EventBatch) error) error {
	return cg.Checkout(func(pc *PartitionConsumer) error {
		cg.logger.Printf("Partition consumer t:%s p:%d o:%d\n", pc.topic, pc.partition, pc.offset)
		if batch := pc.Fetch(); batch != nil {
			cg.logger.Printf("Batch: %q\n", batch)
			// Try to reset offset on OffsetOutOfRange errors
			if batch.offsetIsOutOfRange() {
				if err := cg.Commit(pc.partition, 0); err != nil {
					return err
				}
				cg.force <- true
				batch.Events = batch.Events[:1]
			}

			return callback(batch)
		} else {
			cg.logger.Printf("No batch\n")
		}
		return nil
	})
}

// Commit manually commits an offset for a partition
func (cg *ConsumerGroup) Commit(partition int32, offset int64) error {
	return cg.zoo.Commit(cg.name, cg.topic, partition, offset)
}

// Offset manually retrives an offset for a partition
func (cg *ConsumerGroup) Offset(partition int32) (int64, error) {
	return cg.zoo.Offset(cg.name, cg.topic, partition)
}

// Claims returns the claimed partitions
func (cg *ConsumerGroup) Claims() []int32 {
	res := make([]int32, 0, len(cg.claims))
	for _, claim := range cg.claims {
		res = append(res, claim.partition)
	}
	return res
}

// Close closes the consumer group
func (cg *ConsumerGroup) Close() error {
	close(cg.stopper)
	<-cg.done
	return nil
}

// Background signal loop
func (cg *ConsumerGroup) signalLoop() {
	for {
		// If we have no zk handle, rebalance
		if cg.zkchange == nil {
			if err := cg.rebalance(); err != nil && cg.logger != nil {
				cg.logger.Printf("%s rebalance error: %s", cg.name, err.Error())
			}
		}

		// If rebalace failed, check if we had a stop signal, then try again
		if cg.zkchange == nil {
			select {
			case <-cg.stopper:
				cg.stop()
				return
			case <-time.After(time.Millisecond):
				// Continue
			}
			continue
		}

		// If rebalace worked, wait for a stop signal or a zookeeper change or a fetch-request
		select {
		case <-cg.stopper:
			cg.stop()
			return
		case <-cg.force:
			cg.zkchange = nil
		case <-cg.zkchange:
			cg.zkchange = nil
		case <-cg.checkout:
			cg.claimed <- cg.nextConsumer()
		}
	}
}

/**********************************************************************
 * PRIVATE
 **********************************************************************/

// Stops the consumer group
func (cg *ConsumerGroup) stop() {
	cg.releaseClaims()
	close(cg.done)
}

// Checkout a claimed partition consumer
func (cg *ConsumerGroup) nextConsumer() *PartitionConsumer {
	if len(cg.claims) < 1 {
		return nil
	}

	shift := cg.claims[0]
	cg.claims = append(cg.claims[1:], shift)
	return &shift
}

// Start a rebalance cycle
func (cg *ConsumerGroup) rebalance() (err error) {
	var cids []string
	var pids []int32

	// Fetch a list of consumers and listen for changes
	if cids, cg.zkchange, err = cg.zoo.Consumers(cg.name); err != nil {
		cg.zkchange = nil
		return
	}

	// Fetch a list of partition IDs
	if pids, err = cg.client.Partitions(cg.topic); err != nil {
		cg.zkchange = nil
		return
	}

	// Get leaders for each partition ID
	parts := make(PartitionSlice, len(pids))
	for i, pid := range pids {
		var broker *sarama.Broker
		if broker, err = cg.client.Leader(cg.topic, pid); err != nil {
			cg.zkchange = nil
			return
		}
		defer broker.Close()
		parts[i] = Partition{Id: pid, Addr: broker.Addr()}
	}

	if err = cg.makeClaims(cids, parts); err != nil {
		cg.zkchange = nil
		cg.releaseClaims()
		return
	}
	return
}

func (cg *ConsumerGroup) makeClaims(cids []string, parts PartitionSlice) error {
	cg.releaseClaims()

	for _, part := range cg.claimRange(cids, parts) {
		pc, err := NewPartitionConsumer(cg, part.Id)
		if err != nil {
			return err
		}

		err = cg.zoo.Claim(cg.name, cg.topic, pc.partition, cg.id)
		if err != nil {
			return err
		}

		cg.claims = append(cg.claims, *pc)
	}
	return nil
}

// Determine the partititons dumber to claim
func (cg *ConsumerGroup) claimRange(cids []string, parts PartitionSlice) PartitionSlice {
	sort.Strings(cids)
	sort.Sort(parts)

	cpos := sort.SearchStrings(cids, cg.id)
	clen := len(cids)
	plen := len(parts)
	if cpos >= clen || cpos >= plen {
		return make(PartitionSlice, 0)
	}

	step := int(math.Ceil(float64(plen) / float64(clen)))
	if step < 1 {
		step = 1
	}

	last := (cpos + 1) * step
	if last > plen {
		last = plen
	}
	return parts[cpos*step : last]
}

// Releases all claims
func (cg *ConsumerGroup) releaseClaims() {
	for _, pc := range cg.claims {
		pc.Close()
		cg.zoo.Release(cg.name, cg.topic, pc.partition, cg.id)
	}
	cg.claims = cg.claims[:0]
}

// Validate consumer config, maybe sarama can expose a public ConsumerConfig.Validate() one day
func validateConsumerConfig(config *sarama.ConsumerConfig) error {
	if config.DefaultFetchSize < 0 {
		return sarama.ConfigurationError("Invalid DefaultFetchSize")
	} else if config.DefaultFetchSize == 0 {
		config.DefaultFetchSize = 1024
	}

	if config.MinFetchSize < 0 {
		return sarama.ConfigurationError("Invalid MinFetchSize")
	} else if config.MinFetchSize == 0 {
		config.MinFetchSize = 1
	}

	if config.MaxWaitTime <= 0 {
		return sarama.ConfigurationError("Invalid MaxWaitTime")
	} else if config.MaxWaitTime < 100 {
		sarama.Logger.Println("ConsumerConfig.MaxWaitTime is very low, which can cause high CPU and network usage. See sarama documentation for details.")
	}

	if config.MaxMessageSize < 0 {
		return sarama.ConfigurationError("Invalid MaxMessageSize")
	} else if config.EventBufferSize < 0 {
		return sarama.ConfigurationError("Invalid EventBufferSize")
	}

	return nil
}
