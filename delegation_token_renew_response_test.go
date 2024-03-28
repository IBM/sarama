package sarama

import (
	"testing"
	"time"
)

var (
	renewDelegationTokenResponseV0 = []byte{
		0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, // Expiry time
		0, 0, 0, 0, // Throttle time
	}

	renewDelegationTokenResponseV1 = []byte{
		0, 0,
		0, 0, 0, 0, 0, 54, 238, 128, // Expiry time
		0, 0, 0, 0, // Throttle time
	}

	renewDelegationTokenResponseV2 = []byte{
		0, 0,
		0, 0, 0, 0, 0, 54, 238, 128, // Expiry time
		0, 0, 0, 0, // Throttle time
		0, // Tag buffer
	}
)

func TestRenewDelegationTokenResponse(t *testing.T) {
	resp := &RenewDelegationTokenResponse{ExpiryTime: time.Unix(0, 0)}

	testResponse(t, "version 0", resp, renewDelegationTokenResponseV0)

	resp.Version = 1
	resp.ExpiryTime = resp.ExpiryTime.Add(time.Hour)

	testResponse(t, "version 2", resp, renewDelegationTokenResponseV1)

	resp.Version = 2
	testResponse(t, "version 1", resp, renewDelegationTokenResponseV2)

}
