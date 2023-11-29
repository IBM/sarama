package sarama

import (
	"testing"
)

var (
	expireDelegationTokenRequestV0 = []byte{
		0, 0, 0, 5,
		0, 2, 4, 6, 8,
		0, 0, 0, 0, 0, 0, 0, 0,
	}

	expireDelegationTokenRequestV1 = []byte{
		0, 0, 0, 8,
		0, 2, 4, 6, 8, 10, 12, 14,
		0, 0, 0, 0, 0, 0, 0, 0,
	}

	expireDelegationTokenRequestV2 = []byte{
		9,
		0, 2, 4, 6, 8, 10, 12, 14,
		0, 0, 0, 0, 0, 0, 0, 0,
		0,
	}
)

func TestExpireDelegationTokenRequest(t *testing.T) {
	resp := &ExpireDelegationTokenRequest{HMAC: []byte{0, 2, 4, 6, 8}}

	testRequest(t, "version 0", resp, expireDelegationTokenRequestV0)

	resp.Version = 1
	resp.HMAC = append(resp.HMAC, 10, 12, 14)

	testRequest(t, "version 1", resp, expireDelegationTokenRequestV1)

	resp.Version = 2
	testRequest(t, "version 2", resp, expireDelegationTokenRequestV2)
}
