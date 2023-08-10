package sarama

import (
	"testing"

	"go.uber.org/goleak"
)

var baseSaslRequest = []byte{
	0, 3, 'f', 'o', 'o', // Mechanism
}

func TestSaslHandshakeRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	request := new(SaslHandshakeRequest)
	request.Mechanism = "foo"
	testRequest(t, "basic", request, baseSaslRequest)
}
