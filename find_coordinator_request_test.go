//go:build !functional

package sarama

import "testing"

var (
	findCoordinatorRequestConsumerGroup = []byte{
		0, 5, 'g', 'r', 'o', 'u', 'p',
		0,
	}

	findCoordinatorRequestTransaction = []byte{
		0, 13, 't', 'r', 'a', 'n', 's', 'a', 'c', 't', 'i', 'o', 'n', 'i', 'd',
		1,
	}

	findCoordinatorRequestTransactionV3 = []byte{
		14, 't', 'r', 'a', 'n', 's', 'a', 'c', 't', 'i', 'o', 'n', 'i', 'd',
		1,
		0, // empty tagged fields
	}
)

func TestFindCoordinatorRequest(t *testing.T) {
	req := &FindCoordinatorRequest{
		Version:         1,
		CoordinatorKey:  "group",
		CoordinatorType: CoordinatorGroup,
	}

	testRequest(t, "version 1 - group", req, findCoordinatorRequestConsumerGroup)

	req = &FindCoordinatorRequest{
		Version:         1,
		CoordinatorKey:  "transactionid",
		CoordinatorType: CoordinatorTransaction,
	}

	testRequest(t, "version 1 - transaction", req, findCoordinatorRequestTransaction)

	req.Version = 3
	testRequest(t, "version 3 - transaction", req, findCoordinatorRequestTransactionV3)
}
