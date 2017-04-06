package sarama

import "testing"

var (
	emptyCreateTopicsResponse = []byte{
		0x00, 0x00, 0x00, 0x00}

	twoTopicsCreateTopicsResponse = []byte{
		0x00, 0x00, 0x00, 0x02,

		0x00, 0x03, 'f', 'o', 'o',
		0x00, 0x24,

		0x00, 0x03, 'b', 'a', 'r',
		0x00, 0x00}
)

func TestEmptyCreateTopicsResponse(t *testing.T) {
	response := CreateTopicsResponse{}

	testVersionDecodable(t, "empty", &response, emptyCreateTopicsResponse, 0)
	if len(response.TopicErrorCodes) != 0 {
		t.Error("Decoding produced", len(response.TopicErrorCodes), "topic error codes there were none!")
	}
}

func TestCreateTopicsResponseWithTopics(t *testing.T) {
	response := CreateTopicsResponse{}

	testVersionDecodable(t, "2 topic error codes", &response, twoTopicsCreateTopicsResponse, 0)

	if len(response.TopicErrorCodes) != 2 {
		t.Fatal("Decoding produced", len(response.TopicErrorCodes), "topic error codes where there were two!")
	}

	if response.TopicErrorCodes[0].Topic != "foo" {
		t.Fatal("Decoding produced invalid topic 0 name.")
	}

	if response.TopicErrorCodes[0].ErrorCode != 0x24 {
		t.Fatal("Decoding produced invalid topic 0 error code.")
	}

	if response.TopicErrorCodes[1].Topic != "bar" {
		t.Fatal("Decoding produced invalid topic 1 name.")
	}

	if response.TopicErrorCodes[1].ErrorCode != 0x0 {
		t.Fatal("Decoding produced invalid topic 1 error code.")
	}
}
