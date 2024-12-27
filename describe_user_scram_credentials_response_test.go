//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var (
	emptyDescribeUserScramCredentialsResponse = []byte{
		0, 0, 11, 184, // throttle time (3000 ms)
		0, 0, // no error code
		0, // no error message
		1, // empty array
		0, // tagged fields
	}

	userDescribeUserScramCredentialsResponse = []byte{
		0, 0, 11, 184, // throttle time (3000 ms)
		0, 11, // Error Code
		6, 'e', 'r', 'r', 'o', 'r', // ErrorMessage
		2,                               // Results array length
		7, 'n', 'o', 'b', 'o', 'd', 'y', // User
		0, 13, // User ErrorCode
		11, 'e', 'r', 'r', 'o', 'r', '_', 'u', 's', 'e', 'r', // User ErrorMessage
		2,           // CredentialInfos array length
		2,           // Mechanism
		0, 0, 16, 0, // Iterations
		0, 0, 0,
	}
)

func TestDescribeUserScramCredentialsResponse(t *testing.T) {
	response := &DescribeUserScramCredentialsResponse{
		Version:      0,
		ThrottleTime: time.Second * 3,
	}
	testResponse(t, "empty", response, emptyDescribeUserScramCredentialsResponse)

	responseErrorMessage := "error"
	responseUserErrorMessage := "error_user"

	response.ErrorCode = 11
	response.ErrorMessage = &responseErrorMessage
	response.Results = append(response.Results, &DescribeUserScramCredentialsResult{
		User:         "nobody",
		ErrorCode:    13,
		ErrorMessage: &responseUserErrorMessage,
		CredentialInfos: []*UserScramCredentialsResponseInfo{
			{
				Mechanism:  SCRAM_MECHANISM_SHA_512,
				Iterations: 4096,
			},
		},
	})
	testResponse(t, "empty", response, userDescribeUserScramCredentialsResponse)
}
