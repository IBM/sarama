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
)

func TestCreateAclsResponse(t *testing.T) {
	errmsg := "error"
	resp := &CreateACLsResponse{
		ThrottleTime: 100 * time.Millisecond,
		ACLCreationResponses: []*ACLCreationResponse{{
			Err:    ErrInvalidRequest,
			ErrMsg: &errmsg,
		}},
	}

	testResponse(t, "response with error", resp, createResponseWithError)

	resp.ACLCreationResponses = append(resp.ACLCreationResponses, new(ACLCreationResponse))

	testResponse(t, "response array", resp, createResponseArray)
}
