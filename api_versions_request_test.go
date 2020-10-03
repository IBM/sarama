package sarama

import "testing"

var (
	apiVersionRequest []byte
)

func TestApiVersionsRequest(t *testing.T) {
	request := new(ApiVersionsRequest)
	testRequest(t, "basic", request, apiVersionRequest)
}
