package sarama

import "testing"

var (
	saslAuthenticateRequest = []byte{
		0, 3, 'f', 'o', 'o',
	}
)

func TestSaslAuthenticateRequest(t *testing.T) {
	var request *SaslHandshakeRequest

	request = new(SaslHandshakeRequest)
	request.Mechanism = "foo"
	testRequest(t, "basic", request, saslAuthenticateRequest)
}
