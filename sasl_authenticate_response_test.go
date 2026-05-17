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
	saslAuthenticateResponseErrV2 = []byte{
		0, 58,
		4, 'e', 'r', 'r',
		4, 'm', 's', 'g',
		0, 0, 0, 0, 0, 0, 0, 1,
		0, // empty tagged fields
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

	testResponse(t, "authenticate response v1", response, saslAuthenticateResponseErrV1)
}

func TestSaslAuthenticateResponseV2(t *testing.T) {
	msg := "err"
	response := &SaslAuthenticateResponse{
		Version:           2,
		Err:               ErrSASLAuthenticationFailed,
		ErrorMessage:      &msg,
		SaslAuthBytes:     []byte(`msg`),
		SessionLifetimeMs: 1,
	}

	testResponse(t, "authenticate response v2", response, saslAuthenticateResponseErrV2)
}
