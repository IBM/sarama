//go:build !functional

package sarama

import "testing"

var (
	alterClientQuotasRequestSingleOp = []byte{
		0, 0, 0, 1, // entries len
		0, 0, 0, 1, // entity len
		0, 4, 'u', 's', 'e', 'r', // entity type
		255, 255, // entity value
		0, 0, 0, 1, // ops len
		0, 18, 'p', 'r', 'o', 'd', 'u', 'c', 'e', 'r', '_', 'b', 'y', 't', 'e', '_', 'r', 'a', 't', 'e', // op key
		65, 46, 132, 128, 0, 0, 0, 0, // op value (1000000)
		0, // remove
		0, // validate only
	}

	alterClientQuotasRequestRemoveSingleOp = []byte{
		0, 0, 0, 1, // entries len
		0, 0, 0, 1, // entity len
		0, 4, 'u', 's', 'e', 'r', // entity type
		255, 255, // entity value
		0, 0, 0, 1, // ops len
		0, 18, 'p', 'r', 'o', 'd', 'u', 'c', 'e', 'r', '_', 'b', 'y', 't', 'e', '_', 'r', 'a', 't', 'e', // op key
		0, 0, 0, 0, 0, 0, 0, 0, // op value (ignored)
		1, // remove
		1, // validate only
	}

	alterClientQuotasRequestMultipleOps = []byte{
		0, 0, 0, 1, // entries len
		0, 0, 0, 1, // entity len
		0, 4, 'u', 's', 'e', 'r', // entity type
		255, 255, // entity value
		0, 0, 0, 2, // ops len
		0, 18, 'p', 'r', 'o', 'd', 'u', 'c', 'e', 'r', '_', 'b', 'y', 't', 'e', '_', 'r', 'a', 't', 'e', // op key
		65, 46, 132, 128, 0, 0, 0, 0, // op value (1000000)
		0,                                                                                               // remove
		0, 18, 'c', 'o', 'n', 's', 'u', 'm', 'e', 'r', '_', 'b', 'y', 't', 'e', '_', 'r', 'a', 't', 'e', // op key
		65, 46, 132, 128, 0, 0, 0, 0, // op value (1000000)
		0, // remove
		0, // validate only
	}

	alterClientQuotasRequestMultipleQuotasEntries = []byte{
		0, 0, 0, 2, // entries len
		0, 0, 0, 1, // entity len
		0, 4, 'u', 's', 'e', 'r', // entity type
		255, 255, // entity value
		0, 0, 0, 1, // ops len
		0, 18, 'p', 'r', 'o', 'd', 'u', 'c', 'e', 'r', '_', 'b', 'y', 't', 'e', '_', 'r', 'a', 't', 'e', // op key
		65, 46, 132, 128, 0, 0, 0, 0, // op value (1000000)
		0,          // remove
		0, 0, 0, 1, // entity len
		0, 9, 'c', 'l', 'i', 'e', 'n', 't', '-', 'i', 'd', // entity type
		255, 255, // entity value
		0, 0, 0, 1, // ops len
		0, 18, 'c', 'o', 'n', 's', 'u', 'm', 'e', 'r', '_', 'b', 'y', 't', 'e', '_', 'r', 'a', 't', 'e', // op key
		65, 46, 132, 128, 0, 0, 0, 0, // op value (1000000)
		0, // remove
		0, // validate only
	}
)

func TestAlterClientQuotasRequest(t *testing.T) {
	// default user
	defaultUserComponent := QuotaEntityComponent{
		EntityType: QuotaEntityUser,
		MatchType:  QuotaMatchDefault,
	}

	// default client-id
	defaultClientIDComponent := QuotaEntityComponent{
		EntityType: QuotaEntityClientID,
		MatchType:  QuotaMatchDefault,
	}

	// Add Quota to default user
	op := ClientQuotasOp{
		Key:    "producer_byte_rate",
		Value:  1000000,
		Remove: false,
	}
	entry := AlterClientQuotasEntry{
		Entity: []QuotaEntityComponent{defaultUserComponent},
		Ops:    []ClientQuotasOp{op},
	}
	req := &AlterClientQuotasRequest{
		Entries:      []AlterClientQuotasEntry{entry},
		ValidateOnly: false,
	}
	testRequest(t, "Add single Quota op", req, alterClientQuotasRequestSingleOp)

	// Remove Quota from default user
	op = ClientQuotasOp{
		Key:    "producer_byte_rate",
		Remove: true,
	}
	entry = AlterClientQuotasEntry{
		Entity: []QuotaEntityComponent{defaultUserComponent},
		Ops:    []ClientQuotasOp{op},
	}
	req = &AlterClientQuotasRequest{
		Entries:      []AlterClientQuotasEntry{entry},
		ValidateOnly: true,
	}
	testRequest(t, "Remove single Quota op", req, alterClientQuotasRequestRemoveSingleOp)

	// Add multiple Quotas ops
	op1 := ClientQuotasOp{
		Key:    "producer_byte_rate",
		Value:  1000000,
		Remove: false,
	}
	op2 := ClientQuotasOp{
		Key:    "consumer_byte_rate",
		Value:  1000000,
		Remove: false,
	}
	entry = AlterClientQuotasEntry{
		Entity: []QuotaEntityComponent{defaultUserComponent},
		Ops:    []ClientQuotasOp{op1, op2},
	}
	req = &AlterClientQuotasRequest{
		Entries:      []AlterClientQuotasEntry{entry},
		ValidateOnly: false,
	}
	testRequest(t, "Add multiple Quota ops", req, alterClientQuotasRequestMultipleOps)

	// Add multiple Quotas Entries
	op1 = ClientQuotasOp{
		Key:    "producer_byte_rate",
		Value:  1000000,
		Remove: false,
	}
	entry1 := AlterClientQuotasEntry{
		Entity: []QuotaEntityComponent{defaultUserComponent},
		Ops:    []ClientQuotasOp{op1},
	}
	op2 = ClientQuotasOp{
		Key:    "consumer_byte_rate",
		Value:  1000000,
		Remove: false,
	}
	entry2 := AlterClientQuotasEntry{
		Entity: []QuotaEntityComponent{defaultClientIDComponent},
		Ops:    []ClientQuotasOp{op2},
	}
	req = &AlterClientQuotasRequest{
		Entries:      []AlterClientQuotasEntry{entry1, entry2},
		ValidateOnly: false,
	}
	testRequest(t, "Add multiple Quotas Entries", req, alterClientQuotasRequestMultipleQuotasEntries)
}
