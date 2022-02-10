package sarama

import "testing"

var baseSaslRequest = []byte{
	0, 3, 'f', 'o', 'o', // Mechanism
}

func TestSaslHandshakeRequest(t *testing.T) {
	t.Parallel()
	request := new(SaslHandshakeRequest)
	request.Mechanism = "foo"
	testRequest(t, "basic", request, baseSaslRequest)
}
