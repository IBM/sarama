package sarama

import (
	"testing"

	"go.uber.org/goleak"
)

var saslAuthenticateRequest = []byte{
	0, 0, 0, 3, 'f', 'o', 'o',
}

func TestSaslAuthenticateRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	request := new(SaslAuthenticateRequest)
	request.SaslAuthBytes = []byte(`foo`)
	testRequest(t, "basic", request, saslAuthenticateRequest)
}

func TestSaslAuthenticateRequestV1(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	request := new(SaslAuthenticateRequest)
	request.Version = 1
	request.SaslAuthBytes = []byte(`foo`)
	testRequest(t, "basic", request, saslAuthenticateRequest)
}
