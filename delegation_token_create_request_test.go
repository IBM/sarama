package sarama

import (
	"testing"
	"time"
)

var (
	createDelegationTokenRequestV0 = []byte{
		0, 0, 0, 0, // Renewers
		255, 255, 255, 255, 255, 255, 255, 255, // Max life time
	}

	createDelegationTokenRequestV1 = []byte{
		0, 0, 0, 2,
		0, 4, 'U', 's', 'e', 'r',
		0, 5, 's', 'u', 'p', 'e', 'r',
		0, 4, 'U', 's', 'e', 'r',
		0, 5, 'a', 'd', 'm', 'i', 'n', // Renewers
		255, 255, 255, 255, 255, 255, 255, 255, // Max life time
	}

	createDelegationTokenRequestV2 = []byte{
		3,
		5, 'U', 's', 'e', 'r',
		6, 's', 'u', 'p', 'e', 'r',
		0, // Tag buffer
		5, 'U', 's', 'e', 'r',
		6, 'a', 'd', 'm', 'i', 'n', // Renewers
		0,                                      // Tag buffer
		255, 255, 255, 255, 255, 255, 255, 255, // Max life time
		0, // Tag buffer
	}

	createDelegationTokenRequestV3 = []byte{
		0,                     // Owner principal type
		5, 't', 'e', 's', 't', // Owner principal name
		3,
		5, 'U', 's', 'e', 'r',
		6, 's', 'u', 'p', 'e', 'r',
		0, // Tag buffer
		5, 'U', 's', 'e', 'r',
		6, 'a', 'd', 'm', 'i', 'n', // Renewers
		0,                                      // Tag buffer
		255, 255, 255, 255, 255, 255, 255, 255, // Max life time
		0, // Tag buffer
	}
)

func TestCreateDelegationTokenRequest(t *testing.T) {
	user := AclResourceUser

	resp := &CreateDelegationTokenRequest{
		Renewers:    []Principal{},
		MaxLifetime: -1 * time.Millisecond,
	}

	testRequest(t, "version 0", resp, createDelegationTokenRequestV0)

	resp.Version = 1
	resp.Renewers = []Principal{{user.String(), "super"}, {user.String(), "admin"}}

	testRequest(t, "version 1", resp, createDelegationTokenRequestV1)

	resp.Version = 2
	testRequest(t, "version 2", resp, createDelegationTokenRequestV2)

	resp.Version = 3
	nm := "test"
	resp.OwnerName = &nm

	testRequest(t, "version 3", resp, createDelegationTokenRequestV3)
}
