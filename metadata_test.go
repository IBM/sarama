package sarama

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetadataRefresh(t *testing.T) {
	stepRefresh := make(chan struct{})
	refresh := newMetadataRefresh(func(topics []string) error {
		<-stepRefresh
		return nil
	})

	ch, queued := refresh.refreshOrQueue([]string{"topic1"})
	if queued {
		t.Errorf("It's the first call, it should not be queued")
	}

	ch2, queued := refresh.refreshOrQueue([]string{"topic2", "topic3"})
	if !queued {
		t.Errorf("This one is requesting different topics, it should be queued")
	}

	ch3, queued := refresh.refreshOrQueue([]string{"topic3"})
	if !queued {
		t.Errorf("This one is requesting the same topics as the second one, it should be queued")
	}

	ch4, queued := refresh.refreshOrQueue([]string{"topic4"})
	if !queued {
		t.Errorf("This one is requesting different topics, it should be queued too")
	}

	ch5, queued := refresh.refreshOrQueue([]string{"topic1"})
	if queued {
		t.Errorf("Same topics as the first call, piggy backing on that call, so it's not queued")
	}

	stepRefresh <- struct{}{}
	require.NoError(t, <-ch)
	require.NoError(t, <-ch5)

	require.NoError(t, <-ch2)
	require.NoError(t, <-ch3)
	require.NoError(t, <-ch4)
}

func TestMetadataRefreshConcurrency(t *testing.T) {
	var firstRefreshChans []chan error
	var lock sync.Mutex
	stepRefresh := make(chan struct{})
	refresh := newMetadataRefresh(func(topics []string) error {
		<-stepRefresh
		return nil
	})

	ch, queued := refresh.refreshOrQueue([]string{"topic1"})
	firstRefreshChans = append(firstRefreshChans, ch)
	if queued {
		t.Errorf("First call, should start a refresh")
	}

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		time.Sleep(time.Millisecond)
		go func() {
			defer wg.Done()

			ch, refreshQueued := refresh.refreshOrQueue([]string{"topic1"})
			if refreshQueued {
				t.Errorf("This one should not be queued: they are all requesting the topic that's already started")
			}
			lock.Lock()
			firstRefreshChans = append(firstRefreshChans, ch)
			lock.Unlock()
		}()
	}
	wg.Wait()
	// We have now queued all the refreshes, and they're all blocked with the first one.
	stepRefresh <- struct{}{}
	// Now they are all finished, we can pull from the channels
	for _, ch := range firstRefreshChans {
		require.NoError(t, <-ch)
	}

	ch, queued = refresh.refreshOrQueue([]string{"topic2", "topic3"})
	if queued {
		t.Errorf("This one should not be queued: no refresh is ongoing")
	}
	ch2, queued := refresh.refreshOrQueue([]string{"topic3", "topic4"})
	if !queued {
		t.Errorf("But now there is a refresh ongoing, so this one should be queued")
	}

	stepRefresh <- struct{}{}
	require.NoError(t, <-ch)
	require.NoError(t, <-ch2)
}

func TestMetadataRefreshFiltersSharedTopicErrors(t *testing.T) {
	t.Run("returns only the errors for the requested topics", func(t *testing.T) {
		re := make(refreshError)
		re.addError("topic1", ErrInvalidTopic)
		re.addError("topic2", ErrUnknownTopicOrPartition)
		re.addError("topic3", ErrLeaderNotAvailable)
		err := re.forTopics([]string{"topic1", "topic3"})

		require.ErrorIs(t, err, ErrInvalidTopic)
		require.NotErrorIs(t, err, ErrUnknownTopicOrPartition)
		require.ErrorIs(t, err, ErrLeaderNotAvailable)

		// each wrapped error names the topic it came from
		require.ErrorContains(t, err, "topic1")
		require.NotContains(t, err.Error(), "topic2")
		require.ErrorContains(t, err, "topic3")
	})

	t.Run("piggy-backed callers only see errors for their topics", func(t *testing.T) {
		block := make(chan struct{})
		refresh := newMetadataRefresh(func(topics []string) error {
			<-block
			re := make(refreshError)
			re.addError("topic3", ErrUnknownTopicOrPartition)
			return re
		})

		// the first call starts an in-flight refresh covering all three
		// topics; the next two piggy-back on it for subsets of those topics
		batched, _ := refresh.refreshOrQueue([]string{"topic1", "topic2", "topic3"})
		topic1, _ := refresh.refreshOrQueue([]string{"topic1"})
		topic2, _ := refresh.refreshOrQueue([]string{"topic2"})
		close(block)

		require.ErrorIs(t, errorForTopics([]string{"topic1", "topic2", "topic3"}, <-batched), ErrUnknownTopicOrPartition)
		require.NoError(t, errorForTopics([]string{"topic1"}, <-topic1))
		require.NoError(t, errorForTopics([]string{"topic2"}, <-topic2))
	})
}
