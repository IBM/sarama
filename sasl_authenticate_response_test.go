package sarama

import "testing"

var (
	saslAuthenticatResponseErr = []byte{
		0, 58,
		0, 3, 'e', 'r', 'r',
		0, 0, 0, 3, 'm', 's', 'g',
	}
)

func TestSaslAuthenticateResponse(t *testing.T) {

	response := new(SaslAuthenticateResponse)
	response.Err = ErrSASLAuthenticationFailed
	msg := "err"
	response.ErrorMessage = &msg
	response.SaslAuthBytes = []byte(`msg`)

	testResponse(t, "authenticate reponse", response, saslAuthenticatResponseErr)
}
