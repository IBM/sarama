package sarama

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

var aclDescribeResponseError = []byte{
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

func TestAclDescribeResponse(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
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
}
