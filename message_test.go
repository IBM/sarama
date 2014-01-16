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

	emptyBulkSnappyMessage = []byte{
		180, 47, 53, 209, //CRC
		0x00,                   // magic version byte
		0x02,                   // attribute flags
		0xFF, 0xFF, 0xFF, 0xFF, // key
		0, 0, 0, 42,
		130, 83, 78, 65, 80, 80, 89, 0, // SNAPPY magic
		0, 0, 0, 1, // min version
		0, 0, 0, 1, // default version
		0, 0, 0, 22, 52, 0, 0, 25, 1, 16, 14, 227, 138, 104, 118, 25, 15, 13, 1, 8, 1, 0, 0, 62, 26, 0}
)

func TestMessageEncoding(t *testing.T) {
	message := Message{}
	testEncodable(t, "empty", &message, emptyMessage)

	message.Value = []byte{}
	message.Codec = CompressionGZIP
	testEncodable(t, "empty gzip", &message, emptyGzipMessage)
}

func TestMessageDecoding(t *testing.T) {
	message := Message{}
	testDecodable(t, "empty", &message, emptyMessage)
	if message.Codec != CompressionNone {
		t.Error("Decoding produced compression codec where there was none.")
	}
	if message.Key != nil {
		t.Error("Decoding produced key where there was none.")
	}
	if message.Value != nil {
		t.Error("Decoding produced value where there was none.")
	}

	testDecodable(t, "empty gzip", &message, emptyGzipMessage)
	if message.Codec != CompressionGZIP {
		t.Error("Decoding produced incorrect compression codec (was gzip).")
	}
	if message.Key != nil {
		t.Error("Decoding produced key where there was none.")
	}
	if message.Value == nil || len(message.Value) != 0 {
		t.Error("Decoding produced nil or content-ful value where there was an empty array.")
	}
}

func TestMessageDecodingWithBulkMessages(t *testing.T) {
	message := Message{}
	testDecodable(t, "empty", &message, emptyBulkSnappyMessage)
	if message.Codec != CompressionSnappy {
		t.Errorf("Decoding produced codec %d, but expected %d.", message.Codec, CompressionSnappy)
	}
	if message.Key != nil {
		t.Errorf("Decoding produced key %+v, but none was expected.", message.Key)
	}
	if message.Value != nil {
		t.Errorf("Decoding produced value %+v, but none was expected.", message.Value)
	}
	if message.Set == nil {
		t.Error("Decoding produced no set, but one was expected.")
	} else if len(message.Set.Messages) == 3 {
		t.Errorf("Decoding produced a set with %d messages, but 2 were expected.", len(message.Set.Messages))
	}
}
