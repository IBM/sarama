//go:build !functional

package sarama

import "testing"

var (
	aclCreateRequest = []byte{
		0, 0, 0, 1,
		3, // resource type = group
		0, 5, 'g', 'r', 'o', 'u', 'p',
		0, 9, 'p', 'r', 'i', 'n', 'c', 'i', 'p', 'a', 'l',
		0, 4, 'h', 'o', 's', 't',
		2, // all
		2, // deny
	}
	aclCreateRequestv1 = []byte{
		0, 0, 0, 1,
		3, // resource type = group
		0, 5, 'g', 'r', 'o', 'u', 'p',
		3, // resource pattern type = literal
		0, 9, 'p', 'r', 'i', 'n', 'c', 'i', 'p', 'a', 'l',
		0, 4, 'h', 'o', 's', 't',
		2, // all
		2, // deny
	}
)

func TestCreateAclsRequestv0(t *testing.T) {
	req := &CreateAclsRequest{
		Version: 0,
		AclCreations: []*AclCreation{
			{
				Resource: Resource{
					ResourceType: AclResourceGroup,
					ResourceName: "group",
				},
				Acl: Acl{
					Principal:      "principal",
					Host:           "host",
					Operation:      AclOperationAll,
					PermissionType: AclPermissionDeny,
				},
			},
		},
	}

	testRequest(t, "create request", req, aclCreateRequest)
}

func TestCreateAclsRequestv1(t *testing.T) {
	req := &CreateAclsRequest{
		Version: 1,
		AclCreations: []*AclCreation{
			{
				Resource: Resource{
					ResourceType:        AclResourceGroup,
					ResourceName:        "group",
					ResourcePatternType: AclPatternLiteral,
				},
				Acl: Acl{
					Principal:      "principal",
					Host:           "host",
					Operation:      AclOperationAll,
					PermissionType: AclPermissionDeny,
				},
			},
		},
	}

	testRequest(t, "create request v1", req, aclCreateRequestv1)
}
