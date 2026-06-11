//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var updateFeaturesResponseV0 = []byte{
	0, 0, 0, 100, // throttleTimeMs
	0, 0, // error code
	0,                                    // error message
	2,                                    // Results
	8, 'f', 'e', 'a', 't', 'u', 'r', 'e', // feature
	0, 7, // error code
	6, 'e', 'r', 'r', 'o', 'r', // error message
	0, // empty tagged fields
	0, // empty tagged fields
}

func TestUpdateFeaturesResponse(t *testing.T) {
	errMsg := "error"
	response := &UpdateFeaturesResponse{
		Version:      0,
		ThrottleTime: 100 * time.Millisecond,
		Results: []UpdatableFeatureResult{
			{
				Feature:      "feature",
				ErrorCode:    ErrRequestTimedOut,
				ErrorMessage: &errMsg,
			},
		},
	}

	testResponse(t, "v0", response, updateFeaturesResponseV0)
}
