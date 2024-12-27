//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var (
	emptyAlterUserScramCredentialsResponse = []byte{
		0, 0, 11, 184, // throttle time
		1, // empty results array
		0, // empty tagged fields
	}
	userAlterUserScramCredentialsResponse = []byte{
		0, 0, 11, 184, // throttle time
		2,                               // results array length
		7, 'n', 'o', 'b', 'o', 'd', 'y', // User
		0, 11, // ErrorCode
		6, 'e', 'r', 'r', 'o', 'r', // ErrorMessage
		0, // empty tagged fields
		0, // empty tagged fields
	}
)

func TestAlterUserScramCredentialsResponse(t *testing.T) {
	response := &AlterUserScramCredentialsResponse{
		Version:      0,
		ThrottleTime: time.Second * 3,
	}
	testResponse(t, "empty response", response, emptyAlterUserScramCredentialsResponse)

	resultErrorMessage := "error"
	response.Results = append(response.Results, &AlterUserScramCredentialsResult{
		User:         "nobody",
		ErrorCode:    11,
		ErrorMessage: &resultErrorMessage,
	})
	testResponse(t, "single user response", response, userAlterUserScramCredentialsResponse)
}
