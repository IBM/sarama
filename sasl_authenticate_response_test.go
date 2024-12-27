//go:build !functional

package sarama

import "testing"

var (
	saslAuthenticateResponseErr = []byte{
		0, 58,
		0, 3, 'e', 'r', 'r',
		0, 0, 0, 3, 'm', 's', 'g',
	}
	saslAuthenticateResponseErrV1 = []byte{
		0, 58,
		0, 3, 'e', 'r', 'r',
		0, 0, 0, 3, 'm', 's', 'g',
		0, 0, 0, 0, 0, 0, 0, 1,
	}
)

func TestSaslAuthenticateResponse(t *testing.T) {
	response := new(SaslAuthenticateResponse)
	response.Err = ErrSASLAuthenticationFailed
	msg := "err"
	response.ErrorMessage = &msg
	response.SaslAuthBytes = []byte(`msg`)

	testResponse(t, "authenticate response", response, saslAuthenticateResponseErr)
}

func TestSaslAuthenticateResponseV1(t *testing.T) {
	response := new(SaslAuthenticateResponse)
	response.Err = ErrSASLAuthenticationFailed
	msg := "err"
	response.Version = 1
	response.ErrorMessage = &msg
	response.SaslAuthBytes = []byte(`msg`)
	response.SessionLifetimeMs = 1

	testResponse(t, "authenticate response", response, saslAuthenticateResponseErrV1)
}
