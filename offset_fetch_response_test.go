package sarama

import "testing"

var (
	emptyOffsetFetchResponse = []byte{
		0x00, 0x00, 0x00, 0x00}
	emptyOffsetFetchResponseV2 = []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x2A}
)

func TestEmptyOffsetFetchResponse(t *testing.T) {
	response := OffsetFetchResponse{}
	testResponse(t, "empty", &response, emptyOffsetFetchResponse)

	responseV2 := OffsetFetchResponse{Version: 2, Err: ErrInvalidRequest}
	testResponse(t, "emptyV2", &responseV2, emptyOffsetFetchResponseV2)
}

func TestNormalOffsetFetchResponse(t *testing.T) {
	response := OffsetFetchResponse{}
	response.AddBlock("t", 0, &OffsetFetchResponseBlock{0, "md", ErrRequestTimedOut})
	response.Blocks["m"] = nil
	// The response encoded form cannot be checked for it varies due to
	// unpredictable map traversal order.
	testResponse(t, "normal", &response, nil)

	responseV2 := OffsetFetchResponse{Version: 2, Err: ErrInvalidRequest}
	responseV2.AddBlock("t", 0, &OffsetFetchResponseBlock{0, "md", ErrRequestTimedOut})
	responseV2.Blocks["m"] = nil
	// The response encoded form cannot be checked for it varies due to
	// unpredictable map traversal order.
	testResponse(t, "normalV2", &responseV2, nil)
}
