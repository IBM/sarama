package sarama

import (
	"testing"
)

var (
	aclDescribeRequest = []byte{
		2, // resource type
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 9, 'p', 'r', 'i', 'n', 'c', 'i', 'p', 'a', 'l',
		0, 4, 'h', 'o', 's', 't',
		5, // acl operation
		3, // acl permission type
	}
	aclDescribeRequestV1 = []byte{
		2, // resource type
		0, 5, 't', 'o', 'p', 'i', 'c',
		1, // any Type
		0, 9, 'p', 'r', 'i', 'n', 'c', 'i', 'p', 'a', 'l',
		0, 4, 'h', 'o', 's', 't',
		5, // acl operation
		3, // acl permission type
	}
)

func TestAclDescribeRequestV0(t *testing.T) {
	resourcename := "topic"
	principal := "principal"
	host := "host"

	req := &DescribeAclsRequest{
		AclFilter: AclFilter{
			ResourceType:   AclResourceTopic,
			ResourceName:   &resourcename,
			Principal:      &principal,
			Host:           &host,
			Operation:      AclOperationCreate,
			PermissionType: AclPermissionAllow,
		},
	}

	testRequest(t, "", req, aclDescribeRequest)
}

func TestAclDescribeRequestV1(t *testing.T) {
	resourcename := "topic"
	principal := "principal"
	host := "host"

	req := &DescribeAclsRequest{
		Version: 1,
		AclFilter: AclFilter{
			ResourceType:              AclResourceTopic,
			ResourceName:              &resourcename,
			ResourcePatternTypeFilter: AclPatternAny,
			Principal:                 &principal,
			Host:                      &host,
			Operation:                 AclOperationCreate,
			PermissionType:            AclPermissionAllow,
		},
	}

	testRequest(t, "", req, aclDescribeRequestV1)
}
