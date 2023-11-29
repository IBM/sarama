package sarama

import (
	"testing"
)

var (
	describeDelegationTokenRequestV0 = []byte{
		0, 0, 0, 1,
		0, 4, 'U', 's', 'e', 'r',
		0, 3, 'f', 'o', 'o',
	}

	describeDelegationTokenRequestV1 = []byte{
		0, 0, 0, 2,
		0, 4, 'U', 's', 'e', 'r',
		0, 3, 'f', 'o', 'o',
		0, 4, 'U', 's', 'e', 'r',
		0, 3, 'b', 'a', 'r',
	}

	describeDelegationTokenRequestV2 = []byte{
		3,
		5, 'U', 's', 'e', 'r',
		4, 'f', 'o', 'o',
		0,
		5, 'U', 's', 'e', 'r',
		4, 'b', 'a', 'r',
		0,
		0,
	}
)

func TestDescribeDelegationTokenRequest(t *testing.T) {
	user := AclResourceUser

	resp := &DescribeDelegationTokenRequest{Owners: []Principal{{user.String(), "foo"}}}

	testRequest(t, "version 0", resp, describeDelegationTokenRequestV0)

	resp.Version = 1
	resp.Owners = append(resp.Owners, Principal{user.String(), "bar"})

	testRequest(t, "version 1", resp, describeDelegationTokenRequestV1)

	resp.Version = 2
	testRequest(t, "version 2", resp, describeDelegationTokenRequestV2)

	resp.Version = 3
	testRequest(t, "version 3", resp, describeDelegationTokenRequestV2)
}
