package sarama

import "testing"

var (
	aclDeleteRequestNullsv1 = []byte{
		0, 0, 0, 1,
		1,
		255, 255,
		1, // Any
		255, 255,
		255, 255,
		11,
		3,
	}

	aclDeleteRequestv1 = []byte{
		0, 0, 0, 1,
		1, // any
		0, 6, 'f', 'i', 'l', 't', 'e', 'r',
		1, // Any Filter
		0, 9, 'p', 'r', 'i', 'n', 'c', 'i', 'p', 'a', 'l',
		0, 4, 'h', 'o', 's', 't',
		4, // write
		3, // allow
	}

	aclDeleteRequestNulls = []byte{
		0, 0, 0, 1,
		1,
		255, 255,
		255, 255,
		255, 255,
		11,
		3,
	}

	aclDeleteRequest = []byte{
		0, 0, 0, 1,
		1, // any
		0, 6, 'f', 'i', 'l', 't', 'e', 'r',
		0, 9, 'p', 'r', 'i', 'n', 'c', 'i', 'p', 'a', 'l',
		0, 4, 'h', 'o', 's', 't',
		4, // write
		3, // allow
	}

	aclDeleteRequestArray = []byte{
		0, 0, 0, 2,
		1,
		0, 6, 'f', 'i', 'l', 't', 'e', 'r',
		0, 9, 'p', 'r', 'i', 'n', 'c', 'i', 'p', 'a', 'l',
		0, 4, 'h', 'o', 's', 't',
		4, // write
		3, // allow
		2,
		0, 5, 't', 'o', 'p', 'i', 'c',
		255, 255,
		255, 255,
		6,
		2,
	}
)

func TestDeleteAclsRequest(t *testing.T) {
	req := &DeleteAclsRequest{
		Filters: []*ACLFilter{{
			ResourceType:   ACLResourceAny,
			Operation:      ACLOperationAlterConfigs,
			PermissionType: ACLPermissionAllow,
		}},
	}

	testRequest(t, "delete request nulls", req, aclDeleteRequestNulls)

	req.Filters[0].ResourceName = nullString("filter")
	req.Filters[0].Principal = nullString("principal")
	req.Filters[0].Host = nullString("host")
	req.Filters[0].Operation = ACLOperationWrite

	testRequest(t, "delete request", req, aclDeleteRequest)

	req.Filters = append(req.Filters, &ACLFilter{
		ResourceType:   ACLResourceTopic,
		ResourceName:   nullString("topic"),
		Operation:      ACLOperationDelete,
		PermissionType: ACLPermissionDeny,
	})

	testRequest(t, "delete request array", req, aclDeleteRequestArray)
}

func TestDeleteAclsRequestV1(t *testing.T) {
	req := &DeleteAclsRequest{
		Version: 1,
		Filters: []*ACLFilter{{
			ResourceType:              ACLResourceAny,
			Operation:                 ACLOperationAlterConfigs,
			PermissionType:            ACLPermissionAllow,
			ResourcePatternTypeFilter: ACLPatternAny,
		}},
	}

	testRequest(t, "delete request nulls", req, aclDeleteRequestNullsv1)

	req.Filters[0].ResourceName = nullString("filter")
	req.Filters[0].Principal = nullString("principal")
	req.Filters[0].Host = nullString("host")
	req.Filters[0].Operation = ACLOperationWrite

	testRequest(t, "delete request", req, aclDeleteRequestv1)
}
