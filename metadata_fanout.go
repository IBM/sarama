package sarama

import "sync"

type metadataFanout struct {
	wg             sync.WaitGroup
	cleanupWaiting sync.Once
	initClose      sync.Once
	results        chan *fanoutResult
	closer         chan struct{}
	throttle       chan struct{}

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
		wg:             sync.WaitGroup{},
		cleanupWaiting: sync.Once{},
		initClose:      sync.Once{},
		closer:         make(chan struct{}),
		results:        make(chan *fanoutResult, 1),
		throttle:       make(chan struct{}, maxConcurrentConnections),

		GetResult: make(chan *fanoutResult),
		Done:      make(chan struct{}),
	}
	go f.Listen()
	return f
}

// Fetch is called by the thread initiating the fanout.
func (f *metadataFanout) Fetch(broker metadataFetcher, clientId string, request *MetadataRequest) {
	f.wg.Add(1)
	f.cleanupWaiting.Do(func() { go f.waitAndCleanup(f.Done) })
	go func(worker metadataFetcher) {
		// Wait until throttle allows us to continue or someone else closes our channel
		var checkout struct{}
		select {
		case <-f.closer:
			f.wg.Done()
			return
		case f.throttle <- checkout:
		}

		select {
		case <-f.closer:
			f.wg.Done()
			return
		default:
			r, e := worker.GetMetadata(clientId, request)
			f.results <- &fanoutResult{
				err:      e,
				response: r,
			}
		}
	}(broker)
}

func (f *metadataFanout) waitAndCleanup(doneChan chan struct{}) {
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
	for r := range f.results {
		f.wg.Done()
		lastResult = r
		if r.err == nil {
			f.initClose.Do(func() {
				close(f.closer)
				f.GetResult <- r
			})
		}
		<-f.throttle
	}
	select {
	case <-f.closer:
	default:
		close(f.closer)
		f.GetResult <- lastResult
	}
}
