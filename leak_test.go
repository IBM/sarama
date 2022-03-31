//go:build goleak
// +build goleak

package sarama

import (
	"go.uber.org/goleak"
	"testing"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
}
