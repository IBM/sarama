//go:build !functional

package sarama

import "testing"

var (
	emptyAlterUserScramCredentialsRequest = []byte{
		1, // Deletions
		1, // Upsertions
		0, // empty tagged fields
	}
	userAlterUserScramCredentialsRequest = []byte{
		2,                            // Deletions array, length 1
		7,                            // User name length 6
		'd', 'e', 'l', 'e', 't', 'e', // User name
		2, // SCRAM_SHA_512
		0, // empty tagged fields
		2, // Upsertions array, length 1
		7, // User name length 6
		'u', 'p', 's', 'e', 'r', 't',
		1,           // SCRAM_SHA_256
		0, 0, 16, 0, // iterations: 4096
		// salt bytes:
		6, 119, 111, 114, 108, 100,
		// saltedPassword:
		33, 193, 85, 83, 3, 218, 48, 159, 107, 125, 30, 143,
		228, 86, 54, 191, 221, 220, 75, 245, 100, 5, 231,
		233, 78, 157, 21, 240, 231, 185, 203, 211, 128,
		0, // empty tagged fields
		0, // empty tagged fields
	}
)

func TestAlterUserScramCredentialsRequest(t *testing.T) {
	request := &AlterUserScramCredentialsRequest{
		Version:    0,
		Deletions:  []AlterUserScramCredentialsDelete{},
		Upsertions: []AlterUserScramCredentialsUpsert{},
	}

	// Password is not transmitted, will fail with `testRequest` and `DeepEqual` check
	testRequestEncode(t, "no upsertions/deletions", request, emptyAlterUserScramCredentialsRequest)

	request.Deletions = []AlterUserScramCredentialsDelete{
		{
			Name:      "delete",
			Mechanism: SCRAM_MECHANISM_SHA_512,
		},
	}
	request.Upsertions = []AlterUserScramCredentialsUpsert{
		{
			Name:       "upsert",
			Mechanism:  SCRAM_MECHANISM_SHA_256,
			Iterations: 4096,
			Salt:       []byte("world"),
			Password:   []byte("hello"),
		},
	}
	// Password is not transmitted, will fail with `testRequest` and `DeepEqual` check
	testRequestEncode(t, "single deletion and upsertion", request, userAlterUserScramCredentialsRequest)
}
