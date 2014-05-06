package sarama

import "testing"

var (
	offsetCommitRequestNoGroupNoBlocks = []byte{
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}

	offsetCommitRequestNoBlocks = []byte{
		0x00, 0x06, 'f', 'o', 'o', 'b', 'a', 'r',
		0x00, 0x00, 0x00, 0x00}

	offsetCommitRequestOneBlock = []byte{
		0x00, 0x06, 'f', 'o', 'o', 'b', 'a', 'r',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x05, 't', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x52, 0x21,
		0x00, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0x00, 0x08, 'm', 'e', 't', 'a', 'd', 'a', 't', 'a'}
)

func TestOffsetCommitRequest(t *testing.T) {
	request := new(OffsetCommitRequest)
	testEncodable(t, "no group, no blocks", request, offsetCommitRequestNoGroupNoBlocks)

	request.ConsumerGroup = "foobar"
	testEncodable(t, "no blocks", request, offsetCommitRequestNoBlocks)

	request.AddBlock("topic", 0x5221, 0xDEADBEEF, ReceiveTime, "metadata")
	testEncodable(t, "one block", request, offsetCommitRequestOneBlock)
}
