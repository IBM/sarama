//go:build !functional

package sarama

import "testing"

var saslAuthenticateRequest = []byte{
	0, 0, 0, 3, 'f', 'o', 'o',
}

func TestSaslAuthenticateRequest(t *testing.T) {
	request := new(SaslAuthenticateRequest)
	request.SaslAuthBytes = []byte(`foo`)
	testRequest(t, "basic", request, saslAuthenticateRequest)
}

func TestSaslAuthenticateRequestV1(t *testing.T) {
	request := new(SaslAuthenticateRequest)
	request.Version = 1
	request.SaslAuthBytes = []byte(`foo`)
	testRequest(t, "basic", request, saslAuthenticateRequest)
}
