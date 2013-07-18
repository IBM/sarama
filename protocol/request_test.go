package protocol

import "testing"

var (
	requestSimple = []byte{
		0x00, 0x00, 0x00, 0x16, // msglen
		0x06, 0x66,
		0x00, 0xD2,
		0x00, 0x00, 0x12, 0x34,
		0x00, 0x08, 'm', 'y', 'C', 'l', 'i', 'e', 'n', 't',
		0xDE, 0xAD, 0xBE, 0xEF}
)

type testRequestBody struct {
}

func (s *testRequestBody) key() int16 {
	return 0x666
}

func (s *testRequestBody) version() int16 {
	return 0xD2
}

func (s *testRequestBody) encode(pe packetEncoder) {
	pe.putRaw([]byte{0xDE, 0xAD, 0xBE, 0xEF})
}

func TestRequest(t *testing.T) {
	request := request{correlation_id: 0x1234, id: "myClient", body: new(testRequestBody)}
	testEncodable(t, "simple", &request, requestSimple)
}
