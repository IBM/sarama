package sarama

import (
	"testing"
	"time"
)

var (
	expireDelegationTokenResponseV0 = []byte{
		0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, // Expiry time
		0, 0, 0, 0, // Throttle time
	}

	expireDelegationTokenResponseV1 = []byte{
		0, 0,
		0, 0, 0, 0, 0, 54, 238, 128, // Expiry time
		0, 0, 0, 0, // Throttle time
	}

	expireDelegationTokenResponseV2 = []byte{
		0, 0,
		0, 0, 0, 0, 0, 54, 238, 128, // Expiry time
		0, 0, 0, 0, // Throttle time
		0, // Tag buffer
	}
)

func TestExpireDelegationTokenResponse(t *testing.T) {
	resp := &ExpireDelegationTokenResponse{ExpiryTime: time.Unix(0, 0)}

	testResponse(t, "version 0", resp, expireDelegationTokenResponseV0)

	resp.Version = 1
	resp.ExpiryTime = resp.ExpiryTime.Add(time.Hour)

	testResponse(t, "version 2", resp, expireDelegationTokenResponseV1)

	resp.Version = 2
	testResponse(t, "version 1", resp, expireDelegationTokenResponseV2)

}
