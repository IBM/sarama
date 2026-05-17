//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var (
	aclDescribeResponseError = []byte{
		0, 0, 0, 100,
		0, 8, // error
		0, 5, 'e', 'r', 'r', 'o', 'r',
		0, 0, 0, 1, // 1 resource
		2, // cluster type
		0, 5, 't', 'o', 'p', 'i', 'c',
		0, 0, 0, 1, // 1 acl
		0, 9, 'p', 'r', 'i', 'n', 'c', 'i', 'p', 'a', 'l',
		0, 4, 'h', 'o', 's', 't',
		4, // write
		3, // allow
	}
	aclDescribeResponseErrorV1 = []byte{
		0, 0, 0, 100,
		0, 8, // error
		0, 5, 'e', 'r', 'r', 'o', 'r',
		0, 0, 0, 1, // 1 resource
		2, // cluster type
		0, 5, 't', 'o', 'p', 'i', 'c',
		3,          // pattern type
		0, 0, 0, 1, // 1 acl
		0, 9, 'p', 'r', 'i', 'n', 'c', 'i', 'p', 'a', 'l',
		0, 4, 'h', 'o', 's', 't',
		4, // write
		3, // allow
	}
	aclDescribeResponseErrorV2 = []byte{
		0, 0, 0, 100,
		0, 8, // error
		6, 'e', 'r', 'r', 'o', 'r',
		2, // 1 resource
		2, // cluster type
		6, 't', 'o', 'p', 'i', 'c',
		3, // pattern type
		2, // 1 acl
		10, 'p', 'r', 'i', 'n', 'c', 'i', 'p', 'a', 'l',
		5, 'h', 'o', 's', 't',
		4, // write
		3, // allow
		0, // empty tagged fields
		0, // empty tagged fields
		0, // empty tagged fields
	}
)

func TestAclDescribeResponse(t *testing.T) {
	errmsg := "error"
	resp := &DescribeAclsResponse{
		ThrottleTime: 100 * time.Millisecond,
		Err:          ErrBrokerNotAvailable,
		ErrMsg:       &errmsg,
		ResourceAcls: []*ResourceAcls{{
			Resource: Resource{
				ResourceName: "topic",
				ResourceType: AclResourceTopic,
			},
			Acls: []*Acl{
				{
					Principal:      "principal",
					Host:           "host",
					Operation:      AclOperationWrite,
					PermissionType: AclPermissionAllow,
				},
			},
		}},
	}

	testResponse(t, "describe", resp, aclDescribeResponseError)

	errmsg = ""
	resp = &DescribeAclsResponse{
		ThrottleTime: 100 * time.Millisecond,
		Err:          ErrBrokerNotAvailable,
		ErrMsg:       &errmsg,
		ResourceAcls: []*ResourceAcls{},
	}
	testResponse(t, "describe with empty error message", resp, nil)
}

func TestAclDescribeResponseV1(t *testing.T) {
	errmsg := "error"
	resp := &DescribeAclsResponse{
		Version:      1,
		ThrottleTime: 100 * time.Millisecond,
		Err:          ErrBrokerNotAvailable,
		ErrMsg:       &errmsg,
		ResourceAcls: []*ResourceAcls{{
			Resource: Resource{
				ResourceName:        "topic",
				ResourceType:        AclResourceTopic,
				ResourcePatternType: AclResourcePatternType(3),
			},
			Acls: []*Acl{
				{
					Principal:      "principal",
					Host:           "host",
					Operation:      AclOperationWrite,
					PermissionType: AclPermissionAllow,
				},
			},
		}},
	}

	testResponse(t, "describe", resp, aclDescribeResponseErrorV1)

	errmsg = ""
	resp = &DescribeAclsResponse{
		Version:      1,
		ThrottleTime: 100 * time.Millisecond,
		Err:          ErrBrokerNotAvailable,
		ErrMsg:       &errmsg,
		ResourceAcls: []*ResourceAcls{},
	}
	testResponse(t, "describe with empty error message", resp, nil)
}

func TestAclDescribeResponseV2(t *testing.T) {
	errmsg := "error"
	resp := &DescribeAclsResponse{
		Version:      2,
		ThrottleTime: 100 * time.Millisecond,
		Err:          ErrBrokerNotAvailable,
		ErrMsg:       &errmsg,
		ResourceAcls: []*ResourceAcls{{
			Resource: Resource{
				ResourceName:        "topic",
				ResourceType:        AclResourceTopic,
				ResourcePatternType: AclPatternLiteral,
			},
			Acls: []*Acl{
				{
					Principal:      "principal",
					Host:           "host",
					Operation:      AclOperationWrite,
					PermissionType: AclPermissionAllow,
				},
			},
		}},
	}

	testResponse(t, "describe", resp, aclDescribeResponseErrorV2)

	errmsg = ""
	resp = &DescribeAclsResponse{
		Version:      2,
		ThrottleTime: 100 * time.Millisecond,
		Err:          ErrBrokerNotAvailable,
		ErrMsg:       &errmsg,
		ResourceAcls: []*ResourceAcls{},
	}
	testResponse(t, "describe with empty error message", resp, nil)
}
