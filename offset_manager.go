package sarama

import (
	"sync"
	"time"
)

// Offset Manager

// OffsetManager uses Kafka to store and fetch consumed partition offsets.
type OffsetManager interface {
	// ManagePartition creates a PartitionOffsetManager on the given topic/partition.
	// It will return an error if this OffsetManager is already managing the given
	// topic/partition.
	ManagePartition(topic string, partition int32) (PartitionOffsetManager, error)

	// Close stops the OffsetManager from managing offsets. It is required to call
	// this function before an OffsetManager object passes out of scope, as it
	// will otherwise leak memory. You must call this after all the
	// PartitionOffsetManagers are closed.
	Close() error
}

type offsetManager struct {
	client Client
	conf   *Config
	group  string
	ticker *time.Ticker

	broker   *Broker
	brokerMu sync.RWMutex

	poms   map[string]map[int32]*partitionOffsetManager
	pomsMu sync.Mutex

	closeOnce sync.Once
	closing   chan none
	closed    chan none
}

// NewOffsetManagerFromClient creates a new OffsetManager from the given client.
// It is still necessary to call Close() on the underlying client when finished with the partition manager.
func NewOffsetManagerFromClient(group string, client Client) (OffsetManager, error) {
	// Check that we are not dealing with a closed Client before processing any other arguments
	if client.Closed() {
		return nil, ErrClosedClient
	}

	conf := client.Config()
	om := &offsetManager{
		client: client,
		conf:   conf,
		group:  group,
		ticker: time.NewTicker(conf.Consumer.Offsets.CommitInterval),
		poms:   make(map[string]map[int32]*partitionOffsetManager),

		closing: make(chan none),
		closed:  make(chan none),
	}
	go withRecover(om.mainLoop)

	return om, nil
}

func (om *offsetManager) ManagePartition(topic string, partition int32) (PartitionOffsetManager, error) {
	pom, err := om.newPartitionOffsetManager(topic, partition)
	if err != nil {
		return nil, err
	}

	om.pomsMu.Lock()
	defer om.pomsMu.Unlock()

	topicManagers := om.poms[topic]
	if topicManagers == nil {
		topicManagers = make(map[int32]*partitionOffsetManager)
		om.poms[topic] = topicManagers
	}

	if topicManagers[partition] != nil {
		return nil, ConfigurationError("That topic/partition is already being managed")
	}

	topicManagers[partition] = pom
	return pom, nil
}

func (om *offsetManager) Close() error {
	om.closeOnce.Do(func() {
		// exit the mainLoop
		close(om.closing)
		<-om.closed

		// mark all POMs as closed
		om.asyncClosePOMs()

		// flush one last time
		for retries := om.conf.Metadata.Retry.Max; true; {
			if om.flushToBroker() {
				break
			}
			if retries--; retries < 0 {
				break
			}
		}

		om.releasePOMs(true)
		om.releaseCoordinator()
	})
	return nil
}

func (om *offsetManager) fetchInitialOffset(topic string, partition int32, retries int) (int64, string, error) {
	broker, err := om.coordinator()
	if err != nil {
		if retries <= 0 {
			return 0, "", err
		}
		return om.fetchInitialOffset(topic, partition, retries-1)
	}

	request := new(OffsetFetchRequest)
	request.Version = 1
	request.ConsumerGroup = om.group
	request.AddPartition(topic, partition)

	response, err := broker.FetchOffset(request)
	if err != nil {
		if retries <= 0 {
			return 0, "", err
		}
		om.releaseCoordinator()
		return om.fetchInitialOffset(topic, partition, retries-1)
	}

	block := response.GetBlock(topic, partition)
	if block == nil {
		return 0, "", ErrIncompleteResponse
	}

	switch block.Err {
	case ErrNoError:
		return block.Offset, block.Metadata, nil
	case ErrNotCoordinatorForConsumer:
		if retries <= 0 {
			return 0, "", block.Err
		}
		om.releaseCoordinator()
		return om.fetchInitialOffset(topic, partition, retries-1)
	case ErrOffsetsLoadInProgress:
		if retries <= 0 {
			return 0, "", block.Err
		}
		select {
		case <-om.closing:
			return 0, "", block.Err
		case <-time.After(om.conf.Metadata.Retry.Backoff):
		}
		return om.fetchInitialOffset(topic, partition, retries-1)
	default:
		return 0, "", block.Err
	}
}

func (om *offsetManager) coordinator() (*Broker, error) {
	om.brokerMu.RLock()
	broker := om.broker
	om.brokerMu.RUnlock()

	if broker != nil {
		return broker, nil
	}

	om.brokerMu.Lock()
	defer om.brokerMu.Unlock()

	if broker := om.broker; broker != nil {
		return broker, nil
	}

	if err := om.client.RefreshCoordinator(om.group); err != nil {
		return nil, err
	}

	broker, err := om.client.Coordinator(om.group)
	if err != nil {
		return nil, err
	}

	om.broker = broker
	return broker, nil
}

func (om *offsetManager) releaseCoordinator() {
	om.brokerMu.Lock()
	om.broker = nil
	om.brokerMu.Unlock()
}

func (om *offsetManager) abandonCoordinator() {
	om.brokerMu.Lock()
	broker := om.broker
	om.broker = nil
	om.brokerMu.Unlock()

	if broker != nil {
		_ = broker.Close()
	}
}

func (om *offsetManager) mainLoop() {
	defer om.ticker.Stop()
	defer close(om.closed)

	for {
		select {
		case <-om.ticker.C:
			om.flushToBroker()
			om.releasePOMs(false)
		case <-om.closing:
			return
		}
	}
}

func (om *offsetManager) flushToBroker() (success bool) {
	request := om.constructRequest()
	if request == nil {
		return true
	}

	broker, err := om.coordinator()
	if err != nil {
		om.handleError(err)
		return false
	}

	response, err := broker.CommitOffset(request)
	if err != nil {
		om.abandonCoordinator()
		om.handleError(err)
		return false
	}

	success = true

	om.pomsMu.Lock()
	defer om.pomsMu.Unlock()

	for _, topicManagers := range om.poms {
		for _, pom := range topicManagers {
			if request.blocks[pom.topic] == nil || request.blocks[pom.topic][pom.partition] == nil {
				continue
			}

			var err KError
			var ok bool

			if response.Errors[pom.topic] == nil {
				pom.handleError(ErrIncompleteResponse)
				continue
			}
			if err, ok = response.Errors[pom.topic][pom.partition]; !ok {
				pom.handleError(ErrIncompleteResponse)
				continue
			}

			switch err {
			case ErrNoError:
				block := request.blocks[pom.topic][pom.partition]
				pom.updateCommitted(block.offset, block.metadata)
			case ErrNotLeaderForPartition, ErrLeaderNotAvailable,
				ErrConsumerCoordinatorNotAvailable, ErrNotCoordinatorForConsumer:
				// not a critical error, we just need to redispatch
				om.releaseCoordinator()
			case ErrOffsetMetadataTooLarge, ErrInvalidCommitOffsetSize:
				// nothing we can do about this, just tell the user and carry on
				success = false
				pom.handleError(err)
			case ErrOffsetsLoadInProgress:
				// nothing wrong but we didn't commit, we'll get it next time round
				break
			case ErrUnknownTopicOrPartition:
				// let the user know *and* try redispatching - if topic-auto-create is
				// enabled, redispatching should trigger a metadata request and create the
				// topic; if not then re-dispatching won't help, but we've let the user
				// know and it shouldn't hurt either (see https://github.com/Shopify/sarama/issues/706)
				fallthrough
			default:
				// dunno, tell the user and try redispatching
				success = false
				pom.handleError(err)
				om.releaseCoordinator()
			}
		}
	}
	return
}

func (om *offsetManager) constructRequest() *OffsetCommitRequest {
	var r *OffsetCommitRequest
	var perPartitionTimestamp int64
	if om.conf.Consumer.Offsets.Retention == 0 {
		perPartitionTimestamp = ReceiveTime
		r = &OffsetCommitRequest{
			Version:                 1,
			ConsumerGroup:           om.group,
			ConsumerGroupGeneration: GroupGenerationUndefined,
		}
	} else {
		r = &OffsetCommitRequest{
			Version:                 2,
			RetentionTime:           int64(om.conf.Consumer.Offsets.Retention / time.Millisecond),
			ConsumerGroup:           om.group,
			ConsumerGroupGeneration: GroupGenerationUndefined,
		}

	}

	om.pomsMu.Lock()
	defer om.pomsMu.Unlock()

	for _, topicManagers := range om.poms {
		for _, pom := range topicManagers {
			pom.lock.Lock()
			if pom.dirty {
				r.AddBlock(pom.topic, pom.partition, pom.offset, perPartitionTimestamp, pom.metadata)
			}
			pom.lock.Unlock()
		}
	}

	if len(r.blocks) > 0 {
		return r
	}

	return nil
}

func (om *offsetManager) handleError(err error) {
	om.pomsMu.Lock()
	defer om.pomsMu.Unlock()

	for _, topicManagers := range om.poms {
		for _, pom := range topicManagers {
			pom.handleError(err)
		}
	}
}

func (om *offsetManager) asyncClosePOMs() {
	om.pomsMu.Lock()
	defer om.pomsMu.Unlock()

	for _, topicManagers := range om.poms {
		for _, pom := range topicManagers {
			pom.AsyncClose()
		}
	}
}

func (om *offsetManager) releasePOMs(force bool) {
	om.pomsMu.Lock()
	defer om.pomsMu.Unlock()

	for _, topicManagers := range om.poms {
		for _, pom := range topicManagers {
			pom.lock.Lock()
			releaseDue := pom.done && (force || !pom.dirty)
			pom.lock.Unlock()

			if releaseDue {
				pom.release()

				delete(om.poms[pom.topic], pom.partition)
				if len(om.poms[pom.topic]) == 0 {
					delete(om.poms, pom.topic)
				}
			}
		}
	}
}

// Partition Offset Manager

// PartitionOffsetManager uses Kafka to store and fetch consumed partition offsets. You MUST call Close()
// on a partition offset manager to avoid leaks, it will not be garbage-collected automatically when it passes
// out of scope.
type PartitionOffsetManager interface {
	// NextOffset returns the next offset that should be consumed for the managed
	// partition, accompanied by metadata which can be used to reconstruct the state
	// of the partition consumer when it resumes. NextOffset() will return
	// `config.Consumer.Offsets.Initial` and an empty metadata string if no offset
	// was committed for this partition yet.
	NextOffset() (int64, string)

	// MarkOffset marks the provided offset, alongside a metadata string
	// that represents the state of the partition consumer at that point in time. The
	// metadata string can be used by another consumer to restore that state, so it
	// can resume consumption.
	//
	// To follow upstream conventions, you are expected to mark the offset of the
	// next message to read, not the last message read. Thus, when calling `MarkOffset`
	// you should typically add one to the offset of the last consumed message.
	//
	// Note: calling MarkOffset does not necessarily commit the offset to the backend
	// store immediately for efficiency reasons, and it may never be committed if
	// your application crashes. This means that you may end up processing the same
	// message twice, and your processing should ideally be idempotent.
	MarkOffset(offset int64, metadata string)

	// ResetOffset resets to the provided offset, alongside a metadata string that
	// represents the state of the partition consumer at that point in time. Reset
	// acts as a counterpart to MarkOffset, the difference being that it allows to
	// reset an offset to an earlier or smaller value, where MarkOffset only
	// allows incrementing the offset. cf MarkOffset for more details.
	ResetOffset(offset int64, metadata string)

	// Errors returns a read channel of errors that occur during offset management, if
	// enabled. By default, errors are logged and not returned over this channel. If
	// you want to implement any custom error handling, set your config's
	// Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan *ConsumerError

	// AsyncClose initiates a shutdown of the PartitionOffsetManager. This method will
	// return immediately, after which you should wait until the 'errors' channel has
	// been drained and closed. It is required to call this function, or Close before
	// a consumer object passes out of scope, as it will otherwise leak memory. You
	// must call this before calling Close on the underlying client.
	AsyncClose()

	// Close stops the PartitionOffsetManager from managing offsets. It is required to
	// call this function (or AsyncClose) before a PartitionOffsetManager object
	// passes out of scope, as it will otherwise leak memory. You must call this
	// before calling Close on the underlying client.
	Close() error
}

type partitionOffsetManager struct {
	parent    *offsetManager
	topic     string
	partition int32

	lock     sync.Mutex
	offset   int64
	metadata string
	dirty    bool
	done     bool

	releaseOnce sync.Once
	errors      chan *ConsumerError
}

func (om *offsetManager) newPartitionOffsetManager(topic string, partition int32) (*partitionOffsetManager, error) {
	offset, metadata, err := om.fetchInitialOffset(topic, partition, om.conf.Metadata.Retry.Max)
	if err != nil {
		return nil, err
	}

	return &partitionOffsetManager{
		parent:    om,
		topic:     topic,
		partition: partition,
		errors:    make(chan *ConsumerError, om.conf.ChannelBufferSize),
		offset:    offset,
		metadata:  metadata,
	}, nil
}

func (pom *partitionOffsetManager) Errors() <-chan *ConsumerError {
	return pom.errors
}

func (pom *partitionOffsetManager) MarkOffset(offset int64, metadata string) {
	pom.lock.Lock()
	defer pom.lock.Unlock()

	if offset > pom.offset {
		pom.offset = offset
		pom.metadata = metadata
		pom.dirty = true
	}
}

func (pom *partitionOffsetManager) ResetOffset(offset int64, metadata string) {
	pom.lock.Lock()
	defer pom.lock.Unlock()

	if offset <= pom.offset {
		pom.offset = offset
		pom.metadata = metadata
		pom.dirty = true
	}
}

func (pom *partitionOffsetManager) updateCommitted(offset int64, metadata string) {
	pom.lock.Lock()
	defer pom.lock.Unlock()

	if pom.offset == offset && pom.metadata == metadata {
		pom.dirty = false
	}
}

func (pom *partitionOffsetManager) NextOffset() (int64, string) {
	pom.lock.Lock()
	defer pom.lock.Unlock()

	if pom.offset >= 0 {
		return pom.offset, pom.metadata
	}

	return pom.parent.conf.Consumer.Offsets.Initial, ""
}

func (pom *partitionOffsetManager) AsyncClose() {
	pom.lock.Lock()
	pom.done = true
	pom.lock.Unlock()
}

func (pom *partitionOffsetManager) Close() error {
	pom.AsyncClose()

	var errors ConsumerErrors
	for err := range pom.errors {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

func (pom *partitionOffsetManager) handleError(err error) {
	cErr := &ConsumerError{
		Topic:     pom.topic,
		Partition: pom.partition,
		Err:       err,
	}

	if pom.parent.conf.Consumer.Return.Errors {
		pom.errors <- cErr
	} else {
		Logger.Println(cErr)
	}
}

func (pom *partitionOffsetManager) release() {
	pom.releaseOnce.Do(func() {
		go close(pom.errors)
	})
}
