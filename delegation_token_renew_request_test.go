package sarama

import (
	"testing"
)

var (
	renewDelegationTokenRequestV0 = []byte{
		0, 0, 0, 5,
		0, 2, 4, 6, 8,
		0, 0, 0, 0, 0, 0, 0, 0,
	}

	renewDelegationTokenRequestV1 = []byte{
		0, 0, 0, 8,
		0, 2, 4, 6, 8, 10, 12, 14,
		0, 0, 0, 0, 0, 0, 0, 0,
	}

	renewDelegationTokenRequestV2 = []byte{
		9,
		0, 2, 4, 6, 8, 10, 12, 14,
		0, 0, 0, 0, 0, 0, 0, 0,
		0,
	}
)

func TestRenewDelegationTokenRequest(t *testing.T) {
	resp := &RenewDelegationTokenRequest{HMAC: []byte{0, 2, 4, 6, 8}}

	testRequest(t, "version 0", resp, renewDelegationTokenRequestV0)

	resp.Version = 1
	resp.HMAC = append(resp.HMAC, 10, 12, 14)

	testRequest(t, "version 1", resp, renewDelegationTokenRequestV1)

	resp.Version = 2
	testRequest(t, "version 2", resp, renewDelegationTokenRequestV2)
}
