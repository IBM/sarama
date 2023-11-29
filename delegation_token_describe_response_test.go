package sarama

import (
	"testing"
	"time"
)

var (
	describeDelegationTokenResponseV0 = []byte{
		0, 0,
		0, 0, 0, 2,
		0, 4, 'U', 's', 'e', 'r',
		0, 4, 't', 'e', 's', 't',
		0, 0, 0, 0, 0, 0, 0, 0, // Issue time
		0, 0, 0, 0, 0, 54, 238, 128, // Expiry time
		0, 0, 0, 0, 5, 38, 92, 0, // Max life time
		0, 10, 's', 'o', 'm', 'e', '-', 't', 'o', 'k', 'e', 'n', // Token ID
		0, 0, 0, 4, 1, 2, 3, 4, // HMAC
		0, 0, 0, 0, // Renewers
		0, 4, 'U', 's', 'e', 'r',
		0, 5, 'o', 't', 'h', 'e', 'r',
		0, 0, 0, 0, 0, 0, 0, 0, // Issue time
		0, 0, 0, 0, 0, 54, 238, 128, // Expiry time
		0, 0, 0, 0, 5, 38, 92, 0, // Max life time
		0, 13, 'a', 'n', 'o', 't', 'h', 'e', 'r', '-', 't', 'o', 'k', 'e', 'n', // Token ID
		0, 0, 0, 4, 9, 8, 7, 6, // HMAC
		0, 0, 0, 0, //Renewers
		0, 0, 0, 0, // Throttle time
	}

	describeDelegationTokenResponseV1 = []byte{
		0, 0,
		0, 0, 0, 2,
		0, 4, 'U', 's', 'e', 'r',
		0, 4, 't', 'e', 's', 't',
		0, 0, 0, 0, 0, 0, 0, 0, // Issue time
		0, 0, 0, 0, 0, 54, 238, 128, // Expiry time
		0, 0, 0, 0, 5, 38, 92, 0, // Max life time
		0, 10, 's', 'o', 'm', 'e', '-', 't', 'o', 'k', 'e', 'n', // Token ID
		0, 0, 0, 4, 1, 2, 3, 4, // HMAC
		0, 0, 0, 1,
		0, 4, 'U', 's', 'e', 'r',
		0, 5, 's', 'u', 'p', 'e', 'r', // Renewers
		0, 4, 'U', 's', 'e', 'r',
		0, 5, 'o', 't', 'h', 'e', 'r',
		0, 0, 0, 0, 0, 0, 0, 0, // Issue time
		0, 0, 0, 0, 0, 54, 238, 128, // Expiry time
		0, 0, 0, 0, 5, 38, 92, 0, // Max life time
		0, 13, 'a', 'n', 'o', 't', 'h', 'e', 'r', '-', 't', 'o', 'k', 'e', 'n', // Token ID
		0, 0, 0, 4, 9, 8, 7, 6, // HMAC
		0, 0, 0, 0, // Renewers
		0, 0, 0, 0, // Throttle time
	}

	describeDelegationTokenResponseV2 = []byte{
		0, 0,
		3,
		5, 'U', 's', 'e', 'r',
		5, 't', 'e', 's', 't',
		0, 0, 0, 0, 0, 0, 0, 0, // Issue time
		0, 0, 0, 0, 0, 54, 238, 128, // Expiry time
		0, 0, 0, 0, 5, 38, 92, 0, // Max life time
		11, 's', 'o', 'm', 'e', '-', 't', 'o', 'k', 'e', 'n', // Token ID
		5, 1, 2, 3, 4, // HMAC
		2,
		5, 'U', 's', 'e', 'r',
		6, 's', 'u', 'p', 'e', 'r', // Renewers
		0, // Tag buffer
		0, // Tag buffer
		5, 'U', 's', 'e', 'r',
		6, 'o', 't', 'h', 'e', 'r',
		0, 0, 0, 0, 0, 0, 0, 0, // Issue time
		0, 0, 0, 0, 0, 54, 238, 128, // Expiry time
		0, 0, 0, 0, 5, 38, 92, 0, // Max life time
		14, 'a', 'n', 'o', 't', 'h', 'e', 'r', '-', 't', 'o', 'k', 'e', 'n', // Token ID
		5, 9, 8, 7, 6, // HMAC
		1,          // Renewers
		0,          // Tag buffer
		0, 0, 0, 0, // Throttle time
		0, // Tag buffer
	}

	describeDelegationTokenResponseV3 = []byte{
		0, 0,
		3,
		5, 'U', 's', 'e', 'r',
		5, 't', 'e', 's', 't',
		1, 1,
		0, 0, 0, 0, 0, 0, 0, 0, // Issue time
		0, 0, 0, 0, 0, 54, 238, 128, // Expiry time
		0, 0, 0, 0, 5, 38, 92, 0, // Max life time
		11, 's', 'o', 'm', 'e', '-', 't', 'o', 'k', 'e', 'n', // Token ID
		5, 1, 2, 3, 4, // HMAC
		2,
		5, 'U', 's', 'e', 'r',
		6, 's', 'u', 'p', 'e', 'r', // Renewers
		0, // Tag buffer
		0, // Tag buffer
		5, 'U', 's', 'e', 'r',
		6, 'o', 't', 'h', 'e', 'r',
		5, 'U', 's', 'e', 'r',
		10, 'r', 'e', 'q', 'u', 'e', 's', 't', 'e', 'r',
		0, 0, 0, 0, 0, 0, 0, 0, // Issue time
		0, 0, 0, 0, 0, 54, 238, 128, // Expiry time
		0, 0, 0, 0, 5, 38, 92, 0, // Max life time
		14, 'a', 'n', 'o', 't', 'h', 'e', 'r', '-', 't', 'o', 'k', 'e', 'n', // Token ID
		5, 9, 8, 7, 6, // HMAC
		1,          // Renewers
		0,          // Tag buffer
		0, 0, 0, 0, // Throttle time
		0, // Tag buffer
	}
)

func TestDescribeDelegationTokenResponse(t *testing.T) {
	user := AclResourceUser
	resp := &DescribeDelegationTokenResponse{
		Tokens: []RenewableToken{
			{
				Renewers: []Principal{},
				DelegationToken: DelegationToken{
					Owner:       Principal{user.String(), "test"},
					IssueTime:   time.Unix(0, 0),
					ExpiryTime:  time.Unix(0, 0).Add(time.Hour),
					MaxLifeTime: time.Unix(0, 0).Add(time.Hour * 24),
					TokenID:     "some-token",
					HMAC:        []byte{1, 2, 3, 4},
				},
			},
			{
				Renewers: []Principal{},
				DelegationToken: DelegationToken{
					Owner:       Principal{user.String(), "other"},
					IssueTime:   time.Unix(0, 0),
					ExpiryTime:  time.Unix(0, 0).Add(time.Hour),
					MaxLifeTime: time.Unix(0, 0).Add(time.Hour * 24),
					TokenID:     "another-token",
					HMAC:        []byte{9, 8, 7, 6},
				},
			},
		},
	}

	testResponse(t, "version 0", resp, describeDelegationTokenResponseV0)

	resp.Version = 1
	resp.Tokens[0].Renewers = []Principal{{user.String(), "super"}}

	testResponse(t, "version 1", resp, describeDelegationTokenResponseV1)

	resp.Version = 2
	testResponse(t, "version 2", resp, describeDelegationTokenResponseV2)

	resp.Version = 3
	resp.Tokens[1].Requester = Principal{user.String(), "requester"}

	testResponse(t, "version 3", resp, describeDelegationTokenResponseV3)
}
