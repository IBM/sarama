package sarama

import (
	"sort"
	"sync"
)

type partitionForwarder struct {
	pc PartitionConsumer

	state partitionState
	mutex sync.Mutex

	closed      bool
	dying, dead chan none
}

func newPartitionForwarder(consumer Consumer, topic string, partition int32, info offsetInfo, defaultOffset int64) (*partitionForwarder, error) {
	pc, err := consumer.ConsumePartition(topic, partition, info.NextOffset(defaultOffset))

	// Resume from default offset, if requested offset is out-of-range
	if err == ErrOffsetOutOfRange {
		info.Offset = -1
		pc, err = consumer.ConsumePartition(topic, partition, defaultOffset)
	}
	if err != nil {
		return nil, err
	}

	return &partitionForwarder{
		pc:    pc,
		state: partitionState{Info: info},

		dying: make(chan none),
		dead:  make(chan none),
	}, nil
}

func (pf *partitionForwarder) Loop(messages chan<- *ConsumerMessage, errors chan<- error) {
	for {
		select {
		case msg := <-pf.pc.Messages():
			if msg != nil {
				select {
				case messages <- msg:
				case <-pf.dying:
					close(pf.dead)
					return
				}
			}
		case err := <-pf.pc.Errors():
			if err != nil {
				select {
				case errors <- err:
				case <-pf.dying:
					close(pf.dead)
					return
				}
			}
		case <-pf.dying:
			close(pf.dead)
			return
		}
	}
}

func (pf *partitionForwarder) Close() (err error) {
	if pf.closed {
		return
	}

	pf.closed = true
	close(pf.dying)
	<-pf.dead

	if e := pf.pc.Close(); e != nil {
		err = e
	}
	return
}

func (pf *partitionForwarder) State() partitionState {
	if pf == nil {
		return partitionState{}
	}

	pf.mutex.Lock()
	state := pf.state
	pf.mutex.Unlock()

	return state
}

func (pf *partitionForwarder) MarkCommitted(offset int64) {
	if pf == nil {
		return
	}

	pf.mutex.Lock()
	if offset == pf.state.Info.Offset {
		pf.state.Dirty = false
	}
	pf.mutex.Unlock()
}

func (pf *partitionForwarder) MarkOffset(offset int64, metadata string) {
	if pf == nil {
		return
	}

	pf.mutex.Lock()
	if offset > pf.state.Info.Offset {
		pf.state.Info.Offset = offset
		pf.state.Info.Metadata = metadata
		pf.state.Dirty = true
	}
	pf.mutex.Unlock()
}

// --------------------------------------------------------------------

type partitionState struct {
	Info  offsetInfo
	Dirty bool
}

// --------------------------------------------------------------------

type partitionMap struct {
	data  map[TopicPartition]*partitionForwarder
	mutex sync.RWMutex
}

func newPartitionMap() *partitionMap {
	return &partitionMap{
		data: make(map[TopicPartition]*partitionForwarder),
	}
}

func (m *partitionMap) Fetch(topic string, partition int32) *partitionForwarder {
	m.mutex.RLock()
	pc, _ := m.data[TopicPartition{topic, partition}]
	m.mutex.RUnlock()
	return pc
}

func (m *partitionMap) Store(topic string, partition int32, pf *partitionForwarder) {
	m.mutex.Lock()
	m.data[TopicPartition{topic, partition}] = pf
	m.mutex.Unlock()
}

func (m *partitionMap) HasDirty() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	for _, pf := range m.data {
		if state := pf.State(); state.Dirty {
			return true
		}
	}
	return false
}

func (m *partitionMap) Snapshot() map[TopicPartition]partitionState {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	snap := make(map[TopicPartition]partitionState, len(m.data))
	for tp, pf := range m.data {
		snap[tp] = pf.State()
	}
	return snap
}

func (m *partitionMap) Stop() (err error) {
	m.mutex.RLock()
	size := len(m.data)
	errs := make(chan error, size)
	for tp := range m.data {
		go func(p *partitionForwarder) {
			errs <- p.Close()
		}(m.data[tp])
	}
	m.mutex.RUnlock()

	for i := 0; i < size; i++ {
		if e := <-errs; e != nil {
			err = e
		}
	}
	return
}

func (m *partitionMap) Clear() {
	m.mutex.Lock()
	for tp := range m.data {
		delete(m.data, tp)
	}
	m.mutex.Unlock()
}

func (m *partitionMap) Info() map[string][]int32 {
	info := make(map[string][]int32)
	m.mutex.RLock()
	for tp := range m.data {
		info[tp.Topic] = append(info[tp.Topic], tp.Partition)
	}
	m.mutex.RUnlock()

	for topic := range info {
		sort.Sort(int32Slice(info[topic]))
	}
	return info
}
