package sarama

import (
	"testing"
	"time"
)

var (
	createDelegationTokenResponseV0 = []byte{
		0, 0,
		0, 4, 'U', 's', 'e', 'r',
		0, 4, 't', 'e', 's', 't',
		0, 0, 0, 0, 0, 0, 0, 0, // Issue time
		0, 0, 0, 0, 0, 54, 238, 128, // Expiry time
		0, 0, 0, 0, 5, 38, 92, 0, // Max life time
		0, 10, 's', 'o', 'm', 'e', '-', 't', 'o', 'k', 'e', 'n', // Token ID
		0, 0, 0, 4, 1, 2, 3, 4, // HMAC
		0, 0, 0, 0, // Throttle time
	}

	createDelegationTokenResponseV1 = []byte{
		0, 0,
		0, 4, 'U', 's', 'e', 'r',
		0, 4, 't', 'e', 's', 't',
		0, 0, 0, 0, 0, 0, 0, 0, // Issue time
		0, 0, 0, 0, 0, 109, 221, 0, // Expiry time
		0, 0, 0, 0, 5, 38, 92, 0, // Max life time
		0, 10, 's', 'o', 'm', 'e', '-', 't', 'o', 'k', 'e', 'n', // Token ID
		0, 0, 0, 4, 1, 2, 3, 4, // HMAC
		0, 0, 0, 0, // Throttle time
	}

	createDelegationTokenResponseV2 = []byte{
		0, 0,
		5, 'U', 's', 'e', 'r',
		5, 't', 'e', 's', 't',
		0, 0, 0, 0, 0, 0, 0, 0, // Issue time
		0, 0, 0, 0, 0, 109, 221, 0, // Expiry time
		0, 0, 0, 0, 5, 38, 92, 0, // Max life time
		11, 's', 'o', 'm', 'e', '-', 't', 'o', 'k', 'e', 'n', // Token ID
		5, 1, 2, 3, 4, // HMAC
		0, 0, 0, 0, // Throttle time
		0, // Tag buffer
	}

	createDelegationTokenResponseV3 = []byte{
		0, 0,
		5, 'U', 's', 'e', 'r',
		5, 't', 'e', 's', 't',
		5, 'U', 's', 'e', 'r',
		10, 'r', 'e', 'q', 'u', 'e', 's', 't', 'e', 'r',
		0, 0, 0, 0, 0, 0, 0, 0, // Issue time
		0, 0, 0, 0, 0, 109, 221, 0, // Expiry time
		0, 0, 0, 0, 5, 38, 92, 0, // Max life time
		11, 's', 'o', 'm', 'e', '-', 't', 'o', 'k', 'e', 'n', // Token ID
		5, 1, 2, 3, 4, // HMAC
		0, 0, 0, 0, // Throttle time
		0, // Tag buffer
	}
)

func TestCreateDelegationTokenResponse(t *testing.T) {
	user := AclResourceUser
	resp := &CreateDelegationTokenResponse{
		DelegationToken: DelegationToken{
			Owner:       Principal{user.String(), "test"},
			IssueTime:   time.Unix(0, 0),
			ExpiryTime:  time.Unix(0, 0).Add(time.Hour),
			MaxLifeTime: time.Unix(0, 0).Add(time.Hour * 24),
			TokenID:     "some-token",
			HMAC:        []byte{1, 2, 3, 4},
		},
	}

	testResponse(t, "version 0", resp, createDelegationTokenResponseV0)

	resp.Version = 1
	resp.ExpiryTime = resp.ExpiryTime.Add(time.Hour)

	testResponse(t, "version 1", resp, createDelegationTokenResponseV1)

	resp.Version = 2
	testResponse(t, "version 2", resp, createDelegationTokenResponseV2)

	resp.Version = 3
	resp.Requester = Principal{user.String(), "requester"}

	testResponse(t, "version 3", resp, createDelegationTokenResponseV3)

}
