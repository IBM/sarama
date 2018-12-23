package sarama

import "testing"

var (
	saslAuthenticatResponse = []byte{
		0, 0,
		0, 3, 'e', 'r', 'r',
		0, 0, 0, 3, 'm', 's', 'g',
	}
)

func TestSaslAuthenticateResponse(t *testing.T) {
	var response *SaslAuthenticateResponse

	response = new(SaslAuthenticateResponse)
	testVersionDecodable(t, "no error", response, saslAuthenticatResponse, 0)
	if response.Err != ErrNoError {
		t.Error("Decoding error failed: no error expected but found", response.Err)
	}
	if *response.ErrorMessage != "err" {
		t.Error("Decoding error failed: expected 'err' but found", *response.ErrorMessage)
	}
	if string(response.SaslAuthBytes) != "msg" {
		t.Error("Decoding error failed: expected 'msg' but found", string(response.SaslAuthBytes))
	}
}
