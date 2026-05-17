//go:build !functional

package sarama

import "testing"

var (
	saslAuthenticateRequest = []byte{
		0, 0, 0, 3, 'f', 'o', 'o',
	}
	saslAuthenticateRequestV2 = []byte{
		4, 'f', 'o', 'o',
		0, // empty tagged fields
	}
)

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

func TestSaslAuthenticateRequestV2(t *testing.T) {
	request := new(SaslAuthenticateRequest)
	request.Version = 2
	request.SaslAuthBytes = []byte(`foo`)
	testRequest(t, "basic", request, saslAuthenticateRequestV2)
}
