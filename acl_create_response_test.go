//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var (
	createResponseWithError = []byte{
		0, 0, 0, 100,
		0, 0, 0, 1,
		0, 42,
		0, 5, 'e', 'r', 'r', 'o', 'r',
	}

	createResponseArray = []byte{
		0, 0, 0, 100,
		0, 0, 0, 2,
		0, 42,
		0, 5, 'e', 'r', 'r', 'o', 'r',
		0, 0,
		255, 255,
	}

	createResponseArrayV2 = []byte{
		0, 0, 0, 100,
		3,
		0, 42,
		6, 'e', 'r', 'r', 'o', 'r',
		0, // empty tagged fields
		0, 0,
		0, // nil error message
		0, // empty tagged fields
		0, // empty tagged fields
	}
)

func TestCreateAclsResponse(t *testing.T) {
	errmsg := "error"
	resp := &CreateAclsResponse{
		ThrottleTime: 100 * time.Millisecond,
		AclCreationResponses: []*AclCreationResponse{{
			Err:    ErrInvalidRequest,
			ErrMsg: &errmsg,
		}},
	}

	testResponse(t, "response with error", resp, createResponseWithError)

	resp.AclCreationResponses = append(resp.AclCreationResponses, new(AclCreationResponse))

	testResponse(t, "response array", resp, createResponseArray)

	resp.Version = 2
	testResponse(t, "response array v2", resp, createResponseArrayV2)
}
