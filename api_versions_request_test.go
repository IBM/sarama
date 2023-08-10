package sarama

import (
	"testing"

	"go.uber.org/goleak"
)

var (
	apiVersionRequest []byte

	apiVersionRequestV3 = []byte{
		0x07, 's', 'a', 'r', 'a', 'm', 'a',
		0x07, '0', '.', '1', '0', '.', '0',
		0x00,
	}
)

func TestApiVersionsRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	request := new(ApiVersionsRequest)
	testRequest(t, "basic", request, apiVersionRequest)
}

func TestApiVersionsRequestV3(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	request := new(ApiVersionsRequest)
	request.Version = 3
	request.ClientSoftwareName = "sarama"
	request.ClientSoftwareVersion = "0.10.0"
	testRequest(t, "v3", request, apiVersionRequestV3)
}
