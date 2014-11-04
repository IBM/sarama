package sarama

import (
	"errors"
	"testing"
	"time"
)

type mockMetadataFetcher struct {
	e  error
	id int32
	t  time.Duration
}

func (m *mockMetadataFetcher) GetMetadata(clientId string, request *MetadataRequest) (*MetadataResponse, error) {
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

func TestMetadataFanout(t *testing.T) {
	f := newMetadataFanout()
	expected := fanoutResult{
		response: &MetadataResponse{
			Brokers: []*Broker{
				&Broker{
					id: 1,
				},
			},
		},
	}
	for _, fetch := range []mockMetadataFetcher{
		mockMetadataFetcher{
			id: 1,
			t:  10 * time.Millisecond,
		},
		mockMetadataFetcher{
			id: 2,
			e:  errors.New("timeed out"),
			t:  5 * time.Millisecond,
		},
		mockMetadataFetcher{
			id: 3,
			t:  15 * time.Millisecond,
		},
	} {
		ff := fetch
		f.Fetch(&ff, "id1", &MetadataRequest{Topics: []string{"topics"}})
	}
	result := <-f.Get
	if result.err == expected.err && metadataResponseIdsEqual(result.response, expected.response) {
		t.Errorf("expected %+v but got %+v\n", expected, result)
	}
	<-f.Done
}

func TestMetadataFanoutAllErrored(t *testing.T) {
	f := newMetadataFanout()
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
	for _, fetch := range []mockMetadataFetcher{
		mockMetadataFetcher{
			id: 1,
			e:  errors.New("timeed out"),
			t:  10 * time.Millisecond,
		},
		mockMetadataFetcher{
			id: 2,
			e:  errors.New("timeed out"),
			t:  5 * time.Millisecond,
		},
		mockMetadataFetcher{
			e:  errors.New("timeed out"),
			id: 3,
			t:  15 * time.Millisecond,
		},
	} {
		ff := fetch
		f.Fetch(&ff, "id1", &MetadataRequest{Topics: []string{"topics"}})
	}
	result := <-f.Get
	if result.err == expected.err && metadataResponseIdsEqual(result.response, expected.response) {
		t.Errorf("expected %+v but got %+v\n", expected, result)
	}
	<-f.Done
}
