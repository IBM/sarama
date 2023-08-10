package sarama

import (
	"errors"
	"testing"

	"go.uber.org/goleak"
)

var saslHandshakeResponse = []byte{
	0x00, 0x00,
	0x00, 0x00, 0x00, 0x01,
	0x00, 0x03, 'f', 'o', 'o',
}

func TestSaslHandshakeResponse(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	response := new(SaslHandshakeResponse)
	testVersionDecodable(t, "no error", response, saslHandshakeResponse, 0)
	if !errors.Is(response.Err, ErrNoError) {
		t.Error("Decoding error failed: no error expected but found", response.Err)
	}
	if response.EnabledMechanisms[0] != "foo" {
		t.Error("Decoding error failed: expected 'foo' but found", response.EnabledMechanisms)
	}
}
