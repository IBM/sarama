package sarama

import "testing"

var (
	emptyMessage = []byte{
		167, 236, 104, 3, // CRC
		0x00,                   // magic version byte
		0x00,                   // attribute flags
		0xFF, 0xFF, 0xFF, 0xFF, // key
		0xFF, 0xFF, 0xFF, 0xFF} // value

	emptyGzipMessage = []byte{
		97, 79, 149, 90, //CRC
		0x00,                   // magic version byte
		0x01,                   // attribute flags
		0xFF, 0xFF, 0xFF, 0xFF, // key
		// value
		0x00, 0x00, 0x00, 0x17,
		0x1f, 0x8b,
		0x08,
		0, 0, 9, 110, 136, 0, 255, 1, 0, 0, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0}
)

func TestMessageEncoding(t *testing.T) {
	message := Message{}
	testEncodable(t, "empty", &message, emptyMessage)

	message.Value = []byte{}
	message.Codec = COMPRESSION_GZIP
	testEncodable(t, "empty gzip", &message, emptyGzipMessage)
}

func TestMessageDecoding(t *testing.T) {
	message := Message{}
	testDecodable(t, "empty", &message, emptyMessage)
	if message.Codec != COMPRESSION_NONE {
		t.Error("Decoding produced compression codec where there was none.")
	}
	if message.Key != nil {
		t.Error("Decoding produced key where there was none.")
	}
	if message.Value != nil {
		t.Error("Decoding produced value where there was none.")
	}

	testDecodable(t, "empty gzip", &message, emptyGzipMessage)
	if message.Codec != COMPRESSION_GZIP {
		t.Error("Decoding produced incorrect compression codec (was gzip).")
	}
	if message.Key != nil {
		t.Error("Decoding produced key where there was none.")
	}
	if message.Value == nil || len(message.Value) != 0 {
		t.Error("Decoding produced nil or content-ful value where there was an empty array.")
	}
}
