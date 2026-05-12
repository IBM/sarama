package main

import (
	"encoding/json"

	"github.com/IBM/sarama"
)

// LoadSample is the per-cycle load observation that a member reports to the
// group leader as part of its JoinGroup subscription metadata. The schema is
// versioned so the leader can reject samples it does not understand.
type LoadSample struct {
	Version    int     `json:"v"`
	CPUPercent float64 `json:"cpu"`
	InFlight   int     `json:"in_flight"`
	LagMillis  int64   `json:"lag_ms"`
}

// LoadObserver returns a fresh sample of the local member's current load.
// Implementations should be cheap to call: it runs on every JoinGroup cycle.
type LoadObserver func() LoadSample

// LoadAwareSticky wraps the built-in sticky balance strategy and reports a
// fresh LoadSample to the group leader on every JoinGroup. It implements
// sarama.SubscriptionUserDataBalanceStrategy; assignment logic is delegated unchanged
// to NewBalanceStrategySticky, which keeps this example focused on the
// per-cycle metadata hook rather than on a custom assignor.
//
// A real load-aware assignor would also implement sarama.BalanceStrategy.Plan
// itself, decode each member's UserData on the leader, and weight the
// assignment by the reported load.
type LoadAwareSticky struct {
	sarama.BalanceStrategy
	observe LoadObserver
}

// NewLoadAwareSticky returns a load-aware wrapper around the sticky strategy.
// The observe callback is invoked once per JoinGroup; its return value is
// JSON-serialized into the member's subscription UserData.
func NewLoadAwareSticky(observe LoadObserver) *LoadAwareSticky {
	return &LoadAwareSticky{
		BalanceStrategy: sarama.NewBalanceStrategySticky(),
		observe:         observe,
	}
}

// SubscriptionUserData satisfies sarama.SubscriptionUserDataBalanceStrategy. The
// topics argument is the member's currently subscribed topic set, supplied by
// sarama immediately before the JoinGroup is sent.
func (s *LoadAwareSticky) SubscriptionUserData(_ []string) ([]byte, error) {
	sample := s.observe()
	if sample.Version == 0 {
		sample.Version = 1
	}
	return json.Marshal(sample)
}
