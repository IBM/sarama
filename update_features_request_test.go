//go:build !functional

package sarama

import (
	"testing"
	"time"
)

var updateFeaturesRequestV0 = []byte{
	0, 0, 0, 100, // timeoutMs
	2,                                    // FeatureUpdates
	8, 'f', 'e', 'a', 't', 'u', 'r', 'e', // feature
	0, 2, // max version level
	1, // allow downgrade
	0, // empty tagged fields
	0, // empty tagged fields
}

func TestUpdateFeaturesRequest(t *testing.T) {
	request := &UpdateFeaturesRequest{
		Version: 0,
		Timeout: 100 * time.Millisecond,
		FeatureUpdates: []FeatureUpdate{
			{
				Feature:         "feature",
				MaxVersionLevel: 2,
				AllowDowngrade:  true,
			},
		},
	}

	testRequest(t, "v0", request, updateFeaturesRequestV0)
}
