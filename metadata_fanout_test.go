package sarama

import (
	"errors"
	"testing"
	"time"
)

type mockMetadataFetcher struct {
	e    error
	ranP bool
	id   int32
	t    time.Duration
}

func (m *mockMetadataFetcher) GetMetadata(clientId string, request *MetadataRequest) (*MetadataResponse, error) {
	m.ranP = true
	time.Sleep(m.t)
	if m.e != nil {
		return &MetadataResponse{
			Brokers: []*Broker{
				&Broker{
					id: m.id,
				},
			},
		}, m.e
	}
	return &MetadataResponse{
		Brokers: []*Broker{
			&Broker{
				id: m.id,
			},
		},
	}, nil
}

func metadataResponseIdsEqual(r1 *MetadataResponse, r2 *MetadataResponse) bool {
	if len(r1.Brokers) != len(r2.Brokers) {
		return false
	}
	if len(r1.Brokers) < 1 {
		return true
	}
	for idx, i := range r1.Brokers {
		if i != r2.Brokers[idx] {
			return false
		}
	}
	return true
}

func fetchersRan(fetchers []*mockMetadataFetcher) int {
	i := 0
	for _, fetcher := range fetchers {
		if fetcher.ranP {
			i++
		}
	}
	return i
}

func TestMetadataFanout(t *testing.T) {
	f := newMetadataFanout(3)
	expected := fanoutResult{
		response: &MetadataResponse{
			Brokers: []*Broker{
				&Broker{
					id: 1,
				},
			},
		},
	}
	fetchers := []*mockMetadataFetcher{
		&mockMetadataFetcher{
			id: 1,
			t:  10 * time.Microsecond,
		},
		&mockMetadataFetcher{
			id: 2,
			e:  errors.New("timed out"),
			t:  5 * time.Microsecond,
		},
		&mockMetadataFetcher{
			id: 3,
			t:  15 * time.Microsecond,
		},
	}
	for _, fetch := range fetchers {
		func(ff metadataFetcher) {
			f.Fetch(ff, "id1", &MetadataRequest{Topics: []string{"topics"}})
		}(fetch)
	}
	result := <-f.GetResult
	if result.err == expected.err && metadataResponseIdsEqual(result.response, expected.response) {
		t.Errorf("expected %+v but got %+v\n", expected, result)
	}
	<-f.Done
	if n := fetchersRan(fetchers); n != 3 {
		t.Errorf("expected %d to run but %d ran\n", 3, n)
	}
}

func TestMetadataFanoutAllErrored(t *testing.T) {
	f := newMetadataFanout(3)
	expected := fanoutResult{
		err: errors.New("timeed out"),
		response: &MetadataResponse{
			Brokers: []*Broker{
				&Broker{
					id: 3,
				},
			},
		},
	}
	fetchers := []*mockMetadataFetcher{
		&mockMetadataFetcher{
			id: 1,
			e:  errors.New("timed out"),
			t:  10 * time.Microsecond,
		},
		&mockMetadataFetcher{
			id: 2,
			e:  errors.New("timed out"),
			t:  5 * time.Microsecond,
		},
		&mockMetadataFetcher{
			e:  errors.New("timed out"),
			id: 3,
			t:  15 * time.Microsecond,
		},
	}
	for _, fetch := range fetchers {
		func(ff metadataFetcher) {
			f.Fetch(ff, "id1", &MetadataRequest{Topics: []string{"topics"}})
		}(fetch)
	}
	result := <-f.GetResult
	if result.err == expected.err && metadataResponseIdsEqual(result.response, expected.response) {
		t.Errorf("expected %+v but got %+v\n", expected, result)
	}
	<-f.Done
	if n := fetchersRan(fetchers); n != 3 {
		t.Errorf("expected %d to run but %d ran\n", 3, n)
	}
}

func TestMetadataFanoutSingleConnection(t *testing.T) {
	f := newMetadataFanout(1)
	expected := fanoutResult{
		err: errors.New("timed out"),
		response: &MetadataResponse{
			Brokers: []*Broker{
				&Broker{
					id: 3,
				},
			},
		},
	}
	fetchers := []*mockMetadataFetcher{
		&mockMetadataFetcher{
			id: 1,
			t:  5 * time.Microsecond,
		},
		&mockMetadataFetcher{
			id: 2,
			e:  errors.New("timed out"),
			t:  15 * time.Microsecond,
		},
		&mockMetadataFetcher{
			e:  errors.New("timed out"),
			id: 3,
			t:  15 * time.Microsecond,
		},
	}
	for _, fetch := range fetchers {
		func(ff metadataFetcher) {
			f.Fetch(ff, "id1", &MetadataRequest{Topics: []string{"topics"}})
		}(fetch)
	}
	result := <-f.GetResult
	if result.err == expected.err && metadataResponseIdsEqual(result.response, expected.response) {
		t.Errorf("expected %+v but got %+v\n", expected, result)
	}
	<-f.Done
	if n := fetchersRan(fetchers); n != 1 {
		t.Errorf("expected %d to run but %d ran\n", 1, n)
	}
}
