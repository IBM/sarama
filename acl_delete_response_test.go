//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var (
	deleteAclsResponse = []byte{
		0, 0, 0, 100,
		0, 0, 0, 1,
		0, 0, // no error
		255, 255, // no error message
		0, 0, 0, 1, // 1 matching acl
		0, 0, // no error
		255, 255, // no error message
		2, // resource type
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 9, 'p', 'r', 'i', 'n', 'c', 'i', 'p', 'a', 'l',
		0, 4, 'h', 'o', 's', 't',
		4,
		3,
	}
	deleteAclsResponseV1 = []byte{
		0, 0, 0, 100,
		0, 0, 0, 1,
		0, 0, // no error
		255, 255, // no error message
		0, 0, 0, 1, // 1 matching acl
		0, 0, // no error
		255, 255, // no error message
		2, // resource type
		0, 5, 't', 'o', 'p', 'i', 'c',
		1, // resource pattern type any
		0, 9, 'p', 'r', 'i', 'n', 'c', 'i', 'p', 'a', 'l',
		0, 4, 'h', 'o', 's', 't',
		4,
		3,
	}
	deleteAclsResponseV2 = []byte{
		0, 0, 0, 100,
		2,
		0, 0, // no error
		0,    // no error message
		2,    // 1 matching acl
		0, 0, // no error
		0, // no error message
		2, // resource type
		6, 't', 'o', 'p', 'i', 'c',
		1, // resource pattern type any
		10, 'p', 'r', 'i', 'n', 'c', 'i', 'p', 'a', 'l',
		5, 'h', 'o', 's', 't',
		4,
		3,
		0, // empty tagged fields
		0, // empty tagged fields
		0, // empty tagged fields
	}
)

func TestDeleteAclsResponse(t *testing.T) {
	resp := &DeleteAclsResponse{
		ThrottleTime: 100 * time.Millisecond,
		FilterResponses: []*FilterResponse{{
			MatchingAcls: []*MatchingAcl{{
				Resource: Resource{ResourceType: AclResourceTopic, ResourceName: "topic"},
				Acl:      Acl{Principal: "principal", Host: "host", Operation: AclOperationWrite, PermissionType: AclPermissionAllow},
			}},
		}},
	}

	testResponse(t, "", resp, deleteAclsResponse)
}

func TestDeleteAclsResponseV1(t *testing.T) {
	resp := &DeleteAclsResponse{
		Version:      1,
		ThrottleTime: 100 * time.Millisecond,
		FilterResponses: []*FilterResponse{{
			MatchingAcls: []*MatchingAcl{{
				Resource: Resource{ResourceType: AclResourceTopic, ResourceName: "topic", ResourcePatternType: AclPatternAny},
				Acl:      Acl{Principal: "principal", Host: "host", Operation: AclOperationWrite, PermissionType: AclPermissionAllow},
			}},
		}},
	}

	testResponse(t, "", resp, deleteAclsResponseV1)
}

func TestDeleteAclsResponseV2(t *testing.T) {
	resp := &DeleteAclsResponse{
		Version:      2,
		ThrottleTime: 100 * time.Millisecond,
		FilterResponses: []*FilterResponse{{
			MatchingAcls: []*MatchingAcl{{
				Resource: Resource{ResourceType: AclResourceTopic, ResourceName: "topic", ResourcePatternType: AclPatternAny},
				Acl:      Acl{Principal: "principal", Host: "host", Operation: AclOperationWrite, PermissionType: AclPermissionAllow},
			}},
		}},
	}

	testResponse(t, "", resp, deleteAclsResponseV2)
}
