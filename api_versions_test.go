package sarama

import "testing"

func TestRestrictApiVersion(t *testing.T) {
	t.Run("lowers version to broker max", func(t *testing.T) {
		request := NewMetadataRequest(V2_8_0_0, []string{"test-topic"})

		if request.version() != 10 {
			t.Errorf("Expected MetadataRequest version to be 10, got %d", request.version())
		}

		brokerVersions := apiVersionMap{
			APIKeyMetadata: &apiVersionRange{
				minVersion: 0,
				maxVersion: 8,
			},
		}

		err := restrictApiVersion(request, brokerVersions)
		if err != nil {
			t.Errorf("restrictApiVersion returned unexpected error: %v", err)
		}

		if request.version() != 8 {
			t.Errorf("Expected version to be restricted to 8, got %d", request.version())
		}
	})

	t.Run("leaves version unchanged when within range", func(t *testing.T) {
		request := NewMetadataRequest(V2_4_0_0, []string{"test-topic"})
		originalVersion := request.version()

		if originalVersion != 9 {
			t.Errorf("Expected MetadataRequest version to be 9, got %d", originalVersion)
		}

		brokerVersions := apiVersionMap{
			APIKeyMetadata: &apiVersionRange{
				minVersion: 0,
				maxVersion: 10,
			},
		}

		err := restrictApiVersion(request, brokerVersions)
		if err != nil {
			t.Errorf("restrictApiVersion returned unexpected error: %v", err)
		}

		if request.version() != originalVersion {
			t.Errorf("Expected version to remain %d, got %d", originalVersion, request.version())
		}
	})

	t.Run("does not raise version beyond user-set max", func(t *testing.T) {
		// the Kafka version comes from conf.Version, which is the user-set max Kafka API version to use
		request := NewMetadataRequest(V0_10_0_0, []string{"test-topic"})

		if request.version() != 1 {
			t.Errorf("Expected MetadataRequest version to be 1, got %d", request.version())
		}

		// broker doesn't support versions below 5
		brokerVersions := apiVersionMap{
			APIKeyMetadata: &apiVersionRange{
				minVersion: 5,
				maxVersion: 10,
			},
		}

		// we expect the user's preference to be respected even when it's below the broker's minimum
		err := restrictApiVersion(request, brokerVersions)
		if err != nil {
			t.Errorf("restrictApiVersion returned unexpected error: %v", err)
		}

		if request.version() != 1 {
			t.Errorf("Expected version to be set to minimum 1, got %d", request.version())
		}
	})

	t.Run("does nothing if broker version range missing", func(t *testing.T) {
		request := NewMetadataRequest(V2_8_0_0, []string{"test-topic"})
		originalVersion := request.version()

		brokerVersions := apiVersionMap{
			// no entry for APIKeyMetadata
		}

		err := restrictApiVersion(request, brokerVersions)
		if err != nil {
			t.Errorf("restrictApiVersion returned unexpected error: %v", err)
		}

		if request.version() != originalVersion {
			t.Errorf("Expected version to remain %d, got %d", originalVersion, request.version())
		}
	})
}
