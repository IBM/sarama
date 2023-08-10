package sarama

import (
	"testing"

	"go.uber.org/goleak"
)

var (
	consumerMetadataRequestEmpty = []byte{
		0x00, 0x00,
	}

	consumerMetadataRequestString = []byte{
		0x00, 0x06, 'f', 'o', 'o', 'b', 'a', 'r',
	}
)

func TestConsumerMetadataRequest(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/rcrowley/go-metrics.(*meterArbiter).tick"))
	})
	request := new(ConsumerMetadataRequest)
	testEncodable(t, "empty string", request, consumerMetadataRequestEmpty)
	testVersionDecodable(t, "empty string", request, consumerMetadataRequestEmpty, 0)

	request.ConsumerGroup = "foobar"
	testEncodable(t, "with string", request, consumerMetadataRequestString)
	testVersionDecodable(t, "with string", request, consumerMetadataRequestString, 0)
}
