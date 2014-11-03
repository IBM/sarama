package sarama

import "sync"

type metadataFanout struct {
	wg             sync.WaitGroup
	cleanupWaiting sync.Once
	initClose      sync.Once
	resultChan     chan *fanoutResult
	closeChan      chan struct{}
	// Public channels
	Get chan *fanoutResult
	// optional channel to notify outside when clean up is complete
	Done chan struct{}
}

type fanoutResult struct {
	err      error
	response *MetadataResponse
}

func newMetadataFanout() *metadataFanout {
	f := &Fetcher{
		wg:             sync.WaitGroup{},
		cleanupWaiting: sync.Once{},
		initClose:      sync.Once{},
		closeChan:      make(chan struct{}),
		resultChan:     make(chan *fanoutResult, 1),

		Get:  make(chan *fanoutResult),
		Done: make(chan struct{}),
	}
	go f.Listen()
	return f
}

// Fetch is called by the thread initiating the fanout.
// It "forks" and starts up the described worker.
//
// The Worker should implement its own timeout functionality
//
// TODO what should be the Format for the worker func?
func (f *metadataFanout) Fetch(broker *Broker, clientId string, request *MetadataRequest) {
	f.wg.Add(1)
	f.cleanupWaiting.Do(func() { go f.waitAndCleanup(f.Done) })
	go func(worker func() (string, error)) {
		r, e := broker.GetMetadata(clientId, request)
		select {
		case <-f.closeChan:
			f.wg.Done()
		case f.resultChan <- &fanoutResult{
			err:      e,
			response: r,
		}:
		}
	}(worker)
}

func (f *metadataFanout) waitAndCleanup(doneChan chan struct{}) {
	// Wait until all workers have returned or a successful result has come
	// and workers can shutdown
	f.wg.Wait()
	// then tell the listen loop to close
	close(f.resultChan)
	// and tell the outside we are cleaned up.
	close(doneChan)
}

// Listen validates the results and initiates shutdown if there is a valid result.
// If no valid result is found it emits the last failing result.
func (f *metadataFanout) Listen() {
	var lastResult *fanoutResult
	for r := range f.resultChan {
		f.wg.Done()
		lastResult = r
		if r.err == nil {
			f.initClose.Do(func() {
				close(f.closeChan)
				f.Get <- r
			})
		}
	}
	f.Get <- lastResult
}
