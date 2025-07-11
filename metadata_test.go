package sarama

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetadataRefresh(t *testing.T) {
	var called = make(map[string]int)
	refresh := newSingleFlightRefresher(func(topics []string) error {
		time.Sleep(100 * time.Millisecond)
		// There should never be two refreshes that run concurrently,
		// so it's safe to change the map with no lock.
		for _, topic := range topics {
			called[topic]++
		}
		return nil
	})
	go func() {
		require.NoError(t, refresh([]string{"topic1"}))
	}()
	go func() {
		// This one and the next one will be batched together because the first
		// call is still ongoing when they run.
		// So we will issue a single refresh call, for topic2 and topic3.
		time.Sleep(10 * time.Millisecond)
		require.NoError(t, refresh([]string{"topic2", "topic3"}))
	}()
	go func() {
		time.Sleep(10 * time.Millisecond)
		require.NoError(t, refresh([]string{"topic3", "topic4"}))
	}()
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, refresh([]string{"topic4"}))
	require.Equal(t, 1, called["topic1"])
	require.Equal(t, 1, called["topic2"])
	require.Equal(t, 1, called["topic3"])
	require.Equal(t, 1, called["topic4"])
}

func TestMetadataRefreshConcurrency(t *testing.T) {
	var called = make(map[string]int)
	refresh := newSingleFlightRefresher(func(topics []string) error {
		time.Sleep(100 * time.Millisecond)
		// There should never be two refreshes that run concurrently,
		// so it's safe to change the map with no lock.
		for _, topic := range topics {
			called[topic]++
		}
		return nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		time.Sleep(time.Millisecond)
		go func() {
			defer wg.Done()
			require.NoError(t, refresh([]string{"topic1"}))
		}()
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		time.Sleep(200 * time.Millisecond)
		// Throw in one that doesn't get cached with the other ones
		// because it does not target the same topics.
		require.NoError(t, refresh([]string{"topic2", "topic3"}))
	}()
	go func() {
		defer wg.Done()
		time.Sleep(600 * time.Millisecond)
		// Throw in one that doesn't get cached with the other ones
		// because it does not target the same topics.
		require.NoError(t, refresh([]string{"topic3", "topic4"}))
	}()
	wg.Wait()
	require.LessOrEqual(t, called["topic1"], 20)
	require.Equal(t, called["topic2"], 1)
	require.Equal(t, called["topic3"], 2)
	require.Equal(t, called["topic4"], 1)
}
