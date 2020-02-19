package sarama

import "testing"

var (
	saslAuthenticateRequest = []byte{
		0, 0, 0, 3, 'f', 'o', 'o',
	}
)

func TestSaslAuthenticateRequest(t *testing.T) {
	request := new(SaslAuthenticateRequest)
	request.SaslAuthBytes = []byte(`foo`)
	testRequest(t, "basic", request, saslAuthenticateRequest)
}
