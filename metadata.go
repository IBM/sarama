package sarama

import (
	"sync"
)

type MetadataRefresh func(topics []string) error

// currentRefresh makes sure sarama does not issue metadata requests
// in parallel. If we need to refresh the metadata for a list of topics,
// this struct will check if a refresh is already ongoing, and if so, it will
// accumulate the list of topics to refresh in the next refresh.
// When the current refresh is over, it will queue a new metadata refresh call
// with the accumulated list of topics.
type currentRefresh struct {
	sync.Mutex
	ongoing   bool
	topicsMap map[string]struct{}
	topics    []string
	allTopics bool
	chans     []chan error

	// This is the function that gets called when to refresh the metadata.
	// It is called with the list of all topics that need to be refreshed
	// or with nil if all topics need to be refreshed.
	refresh func(topics []string) error
}

func newCurrentRefresh(f func(topics []string) error) *currentRefresh {
	mr := &currentRefresh{
		topicsMap: make(map[string]struct{}),
		topics:    make([]string, 0),
		refresh:   f,
		chans:     make([]chan error, 0),
	}
	return mr
}

// AddTopicsFrom adds topics from the next refresh to the current refresh.
// You need to hold the lock to call this method.
func (r *currentRefresh) AddTopicsFrom(next *nextRefresh) {
	if next.allTopics {
		r.allTopics = true
		return
	}
	if len(next.topics) > 0 {
		r.AddTopics(next.topics)
		next.topics = next.topics[:0]
	}
}

// nextRefresh holds the list of topics we will need
// to refresh in the next refresh.
// When a refresh is ongoing, calls to RefreshMetadata() are
// accumulated in this struct, so that we can immediately issue another
// refresh when the current refresh is over.
type nextRefresh struct {
	sync.Mutex
	topics    []string
	allTopics bool
}

// AddTopics adds topics to the refresh.
// You need to hold the lock to call this method.
func (r *currentRefresh) AddTopics(topics []string) {
	if len(topics) == 0 {
		r.allTopics = true
		return
	}
	for _, topic := range topics {
		if _, ok := r.topicsMap[topic]; ok {
			continue
		}
		r.topicsMap[topic] = struct{}{}
		r.topics = append(r.topics, topic)
	}
}

func (r *nextRefresh) AddTopics(topics []string) {
	if len(topics) == 0 {
		r.allTopics = true
		return
	}
	r.topics = append(r.topics, topics...)
}

func (r *currentRefresh) HasTopics(topics []string) bool {
	if len(topics) == 0 {
		// This means that the caller wants to know if the refresh is for all topics.
		// In this case, we return true if the refresh is for all topics, or false if it is not.
		return r.allTopics
	}
	if r.allTopics {
		return true
	}
	for _, topic := range topics {
		if _, ok := r.topicsMap[topic]; !ok {
			return false
		}
	}
	return true
}

// Start starts a new refresh.
// The refresh is started in a new goroutine, and this function
// returns a channel on which the caller can wait for the refresh
// to complete.
// You need to hold the lock to call this method.
func (r *currentRefresh) Start() chan error {
	r.ongoing = true
	var ch = make(chan error, 1)
	r.chans = append(r.chans, ch)
	var topics = r.topics
	if r.allTopics {
		topics = nil
	}
	go func() {
		err := r.refresh(topics)
		r.Lock()
		r.ongoing = false
		for _, ch := range r.chans {
			ch <- err
			close(ch)
		}
		r.clear()
		r.Unlock()
	}()
	return ch
}

// clear clears the refresh state.
// You need to hold the lock to call this method.
func (r *currentRefresh) clear() {
	r.topics = r.topics[:0]
	for key := range r.topicsMap {
		delete(r.topicsMap, key)
	}
	r.allTopics = false
	r.chans = r.chans[:0]
}

// Wait returns the channel on which you can wait for the refresh
// to complete.
// You need to hold the lock to call this method.
func (r *currentRefresh) Wait() chan error {
	if !r.ongoing {
		panic("waiting for a refresh that is not ongoing")
	}
	var ch = make(chan error, 1)
	r.chans = append(r.chans, ch)
	return ch
}

// Ongoing returns true if the refresh is ongoing.
// You need to hold the lock to call this method.
func (r *currentRefresh) Ongoing() bool {
	return r.ongoing
}

// singleFlightMetadataRefresher helps managing metadata refreshes.
// It makes sure a sarama client never issues more than one metadata refresh
// in parallel.
type singleFlightMetadataRefresher struct {
	current *currentRefresh
	next    *nextRefresh
}

func newSingleFlightRefresher(f func(topics []string) error) MetadataRefresh {
	var refresher = &singleFlightMetadataRefresher{
		current: newCurrentRefresh(f),
		next: &nextRefresh{
			topics: make([]string, 0),
		},
	}
	return refresher.Refresh
}

// Refresh is the function that clients call when they want to refresh
// the metadata. This function blocks until a refresh is issued, and its
// result is received, for the list of topics the caller provided.
// If a refresh was already ongoing for this list of topics, the function
// waits on that refresh to complete, and returns its result.
// If a refresh was already ongoing for a different list of topics, the function
// accumulates the list of topics to refresh in the next refresh, and queues that refresh.
// If no refresh is ongoing, it will start a new refresh, and return its result.
func (m *singleFlightMetadataRefresher) Refresh(topics []string) error {
	for {
		var current = m.current
		current.Lock()
		if !current.Ongoing() {
			// If no refresh is ongoing, we can start a new one, in which
			// we add the topics that have been accumulated in the next refresh
			// and the topics that have been provided by the caller.
			m.next.Lock()
			m.current.AddTopicsFrom(m.next)
			m.next.Unlock()
			current.AddTopics(topics)
			var ch = current.Start()
			current.Unlock()
			return <-ch
		}
		if current.HasTopics(topics) {
			// A refresh is ongoing, and we were lucky: it is refreshing the topics we need already:
			// we just have to wait for it to finish and return its results.
			var ch = current.Wait()
			current.Unlock()
			var err = <-ch
			return err
		}
		// There is a refresh ongoing, but it is not refreshing the topics we need.
		// We need to wait for it to finish, and then start a new refresh.
		var ch = current.Wait()
		current.Unlock()
		m.next.Lock()
		m.next.AddTopics(topics)
		m.next.Unlock()
		// This is where we wait for that refresh to finish, and the loop will take care
		// of starting the new one.
		<-ch
	}
}
