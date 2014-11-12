package sarama

import "sync"

type metadataFanout struct {
	wg       sync.WaitGroup
	results  chan *fanoutResult
	closer   chan struct{}
	throttle chan struct{}

	GetResult chan *fanoutResult // Stores the first non-error result from the metadata fanout.
	Done      chan struct{}      // optional channel to notify outside when clean up is complete.

}

type fanoutResult struct {
	err      error
	response *MetadataResponse
}

type metadataFetcher interface {
	GetMetadata(string, *MetadataRequest) (*MetadataResponse, error)
}

func newMetadataFanout(maxConcurrentConnections int) *metadataFanout {
	f := &metadataFanout{
		closer:   make(chan struct{}),
		results:  make(chan *fanoutResult, 1),
		throttle: make(chan struct{}, maxConcurrentConnections),

		GetResult: make(chan *fanoutResult),
		Done:      make(chan struct{}),
	}
	go f.Listen()
	return f
}

// Fetch is called by the thread initiating the fanout.
func (f *metadataFanout) Fetch(broker metadataFetcher, clientId string, request *MetadataRequest) {
	f.wg.Add(1)
	go func(worker metadataFetcher) {
		// Wait until throttle allows us to continue or someone else closes our channel
		var checkout struct{}
		select {
		case <-f.closer:
			f.wg.Done()
			return
		case f.throttle <- checkout:
		}

		// Since the above select block can potentially choose the second case even if f.closer is closed
		// we double check that case here. See https://goo.gl/lqydfp
		select {
		case <-f.closer:
			f.wg.Done()
			return
		default:
		}

		r, e := worker.GetMetadata(clientId, request)
		select {
		case <-f.closer:
			f.wg.Done()
			return
		case f.results <- &fanoutResult{
			err:      e,
			response: r,
		}:
		}
	}(broker)
}

func (f *metadataFanout) WaitAndCleanup() {
	// Wait until all workers have returned or a successful result has come
	// and workers can shutdown
	f.wg.Wait()
	// then tell the listen loop to close
	close(f.results)
	// and tell the outside we are cleaned up.
	close(f.Done)
}

// Listen validates the results and initiates shutdown if there is a valid result.
// If no valid result is found it emits the last failing result.
func (f *metadataFanout) Listen() {
	var lastResult *fanoutResult
	goodResultFound := false
	for r := range f.results {
		lastResult = r
		if r.err == nil && !goodResultFound {
			close(f.closer)
			f.GetResult <- r
			goodResultFound = true
		}
		<-f.throttle
		f.wg.Done()
	}
	select {
	case <-f.closer:
	default:
		close(f.closer)
		f.GetResult <- lastResult
	}
}
