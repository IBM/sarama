package sarama

import "testing"

var (
	apiVersionRequest = []byte{}
)

func TestApiVersionsRequest(t *testing.T) {
	var request *APIVersionsRequest

	request = new(APIVersionsRequest)
	testRequest(t, "basic", request, apiVersionRequest)
}
