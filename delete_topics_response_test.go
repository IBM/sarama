package sarama

import "testing"

var (
	emptyDeleteTopicsResponse = []byte{
		0x00, 0x00, 0x00, 0x00}

	twoTopicsDeleteTopicsResponse = []byte{
		0x00, 0x00, 0x00, 0x02,

		0x00, 0x03, 'f', 'o', 'o',
		0x00, 0x24,

		0x00, 0x03, 'b', 'a', 'r',
		0x00, 0x00}
)

func TestEmptyDeleteTopicsResponse(t *testing.T) {
	response := DeleteTopicsResponse{}

	testVersionDecodable(t, "empty", &response, emptyDeleteTopicsResponse, 0)
	if len(response.TopicErrorCodes) != 0 {
		t.Error("Decoding produced", len(response.TopicErrorCodes), "topic error codes there were none!")
	}
}

func TestDeleteTopicsResponseWithTopics(t *testing.T) {
	response := DeleteTopicsResponse{}

	testVersionDecodable(t, "2 topic error codes", &response, twoTopicsDeleteTopicsResponse, 0)

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
