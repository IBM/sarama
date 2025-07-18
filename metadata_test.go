package sarama

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetadataRefresh(t *testing.T) {
	releaseRefresh := make(chan struct{})
	refresh := newMetadataRefresh(func(topics []string) error {
		<-releaseRefresh
		return nil
	})

	ch, queued := refresh.refreshOrQueue([]string{"topic1"})
	// It's the first call, it's not queued.
	require.False(t, queued)

	// This one is requesting different topics, so it's queued.
	ch2, queued := refresh.refreshOrQueue([]string{"topic2", "topic3"})
	require.True(t, queued)

	// This one is requesting the same topics as the second one => queued.
	ch3, queued := refresh.refreshOrQueue([]string{"topic3"})
	require.True(t, queued)

	// This one is requesting different topics, so it's queued.
	ch4, queued := refresh.refreshOrQueue([]string{"topic4"})
	require.True(t, queued)

	// Same topics as the first call, piggy backing on that call, so it's not queued.
	ch5, queued := refresh.refreshOrQueue([]string{"topic1"})
	require.False(t, queued)

	releaseRefresh <- struct{}{}
	require.NoError(t, <-ch)
	require.NoError(t, <-ch5)

	require.NoError(t, <-ch2)
	require.NoError(t, <-ch3)
	require.NoError(t, <-ch4)
}

func TestMetadataRefreshConcurrency(t *testing.T) {
	var firstRefreshChans []chan error
	var lock sync.Mutex
	releaseRefresh := make(chan struct{})
	refresh := newMetadataRefresh(func(topics []string) error {
		<-releaseRefresh
		return nil
	})

	ch, queued := refresh.refreshOrQueue([]string{"topic1"})
	firstRefreshChans = append(firstRefreshChans, ch)
	// This refresh starts a refresh.
	require.False(t, queued)

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		time.Sleep(time.Millisecond)
		go func() {
			defer wg.Done()

			ch, refreshQueued := refresh.refreshOrQueue([]string{"topic1"})
			require.False(t, refreshQueued)
			lock.Lock()
			firstRefreshChans = append(firstRefreshChans, ch)
			lock.Unlock()
		}()
	}
	wg.Wait()
	// We have now queued all the refreshes, and they're all blocked with the first one.
	releaseRefresh <- struct{}{}
	// Now they are all finished, we can pull from the channels
	for _, ch := range firstRefreshChans {
		require.NoError(t, <-ch)
	}

	// This one should not be queued: no refresh is ongoing.
	ch, queued = refresh.refreshOrQueue([]string{"topic2", "topic3"})
	require.False(t, queued)
	// But now there is a refresh ongoing, so this one should be queued.
	ch2, queued := refresh.refreshOrQueue([]string{"topic3", "topic4"})
	require.True(t, queued)

	releaseRefresh <- struct{}{}
	require.NoError(t, <-ch)
	require.NoError(t, <-ch2)
}
