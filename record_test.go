package sarama

import (
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

var recordBatchTestCases = []struct {
	name         string
	batch        RecordBatch
	encoded      []byte
	oldGoEncoded []byte // used in case of gzipped content for go versions prior to 1.8
}{
	{
		name:  "empty record",
		batch: RecordBatch{Version: 2, Records: []*Record{}},
		encoded: []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // First Offset
			0, 0, 0, 49, // Length
			0, 0, 0, 0, // Partition Leader Epoch
			2,                // Version
			89, 95, 183, 221, // CRC
			0, 0, // Attributes
			0, 0, 0, 0, // Last Offset Delta
			0, 0, 0, 0, 0, 0, 0, 0, // First Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // Max Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // Producer ID
			0, 0, // Producer Epoch
			0, 0, 0, 0, // First Sequence
			0, 0, 0, 0, // Number of Records
		},
	},
	{
		name:  "control batch",
		batch: RecordBatch{Version: 2, Control: true, Records: []*Record{}},
		encoded: []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // First Offset
			0, 0, 0, 49, // Length
			0, 0, 0, 0, // Partition Leader Epoch
			2,               // Version
			81, 46, 67, 217, // CRC
			0, 32, // Attributes
			0, 0, 0, 0, // Last Offset Delta
			0, 0, 0, 0, 0, 0, 0, 0, // First Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // Max Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // Producer ID
			0, 0, // Producer Epoch
			0, 0, 0, 0, // First Sequence
			0, 0, 0, 0, // Number of Records
		},
	},
	{
		name: "uncompressed record",
		batch: RecordBatch{
			Version:        2,
			FirstTimestamp: 10,
			Records: []*Record{{
				TimestampDelta: 5,
				Key:            []byte{1, 2, 3, 4},
				Value:          []byte{5, 6, 7},
				Headers: []*RecordHeader{{
					Key:   []byte{8, 9, 10},
					Value: []byte{11, 12},
				}},
			}},
		},
		encoded: []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // First Offset
			0, 0, 0, 70, // Length
			0, 0, 0, 0, // Partition Leader Epoch
			2,                // Version
			219, 71, 20, 201, // CRC
			0, 0, // Attributes
			0, 0, 0, 0, // Last Offset Delta
			0, 0, 0, 0, 0, 0, 0, 10, // First Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // Max Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // Producer ID
			0, 0, // Producer Epoch
			0, 0, 0, 0, // First Sequence
			0, 0, 0, 1, // Number of Records
			40, // Record Length
			0,  // Attributes
			10, // Timestamp Delta
			0,  // Offset Delta
			8,  // Key Length
			1, 2, 3, 4,
			6, // Value Length
			5, 6, 7,
			2,        // Number of Headers
			6,        // Header Key Length
			8, 9, 10, // Header Key
			4,      // Header Value Length
			11, 12, // Header Value
		},
	},
	{
		name: "gzipped record",
		batch: RecordBatch{
			Version:        2,
			Codec:          CompressionGZIP,
			FirstTimestamp: 10,
			Records: []*Record{{
				TimestampDelta: 5,
				Key:            []byte{1, 2, 3, 4},
				Value:          []byte{5, 6, 7},
				Headers: []*RecordHeader{{
					Key:   []byte{8, 9, 10},
					Value: []byte{11, 12},
				}},
			}},
		},
		encoded: []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // First Offset
			0, 0, 0, 94, // Length
			0, 0, 0, 0, // Partition Leader Epoch
			2,                // Version
			15, 156, 184, 78, // CRC
			0, 1, // Attributes
			0, 0, 0, 0, // Last Offset Delta
			0, 0, 0, 0, 0, 0, 0, 10, // First Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // Max Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // Producer ID
			0, 0, // Producer Epoch
			0, 0, 0, 0, // First Sequence
			0, 0, 0, 1, // Number of Records
			31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 210, 96, 224, 98, 224, 96, 100, 98, 102, 97, 99, 101,
			99, 103, 98, 227, 224, 228, 98, 225, 230, 1, 4, 0, 0, 255, 255, 173, 201, 88, 103, 21, 0, 0, 0,
		},
		oldGoEncoded: []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // First Offset
			0, 0, 0, 94, // Length
			0, 0, 0, 0, // Partition Leader Epoch
			2,               // Version
			144, 168, 0, 33, // CRC
			0, 1, // Attributes
			0, 0, 0, 0, // Last Offset Delta
			0, 0, 0, 0, 0, 0, 0, 10, // First Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // Max Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // Producer ID
			0, 0, // Producer Epoch
			0, 0, 0, 0, // First Sequence
			0, 0, 0, 1, // Number of Records
			31, 139, 8, 0, 0, 9, 110, 136, 0, 255, 210, 96, 224, 98, 224, 96, 100, 98, 102, 97, 99, 101,
			99, 103, 98, 227, 224, 228, 98, 225, 230, 1, 4, 0, 0, 255, 255, 173, 201, 88, 103, 21, 0, 0, 0,
		},
	},
	{
		name: "snappy compressed record",
		batch: RecordBatch{
			Version:        2,
			Codec:          CompressionSnappy,
			FirstTimestamp: 10,
			Records: []*Record{{
				TimestampDelta: 5,
				Key:            []byte{1, 2, 3, 4},
				Value:          []byte{5, 6, 7},
				Headers: []*RecordHeader{{
					Key:   []byte{8, 9, 10},
					Value: []byte{11, 12},
				}},
			}},
		},
		encoded: []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // First Offset
			0, 0, 0, 72, // Length
			0, 0, 0, 0, // Partition Leader Epoch
			2,               // Version
			95, 173, 35, 17, // CRC
			0, 2, // Attributes
			0, 0, 0, 0, // Last Offset Delta
			0, 0, 0, 0, 0, 0, 0, 10, // First Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // Max Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // Producer ID
			0, 0, // Producer Epoch
			0, 0, 0, 0, // First Sequence
			0, 0, 0, 1, // Number of Records
			21, 80, 40, 0, 10, 0, 8, 1, 2, 3, 4, 6, 5, 6, 7, 2, 6, 8, 9, 10, 4, 11, 12,
		},
	},
	{
		name: "lz4 compressed record",
		batch: RecordBatch{
			Version:        2,
			Codec:          CompressionLZ4,
			FirstTimestamp: 10,
			Records: []*Record{{
				TimestampDelta: 5,
				Key:            []byte{1, 2, 3, 4},
				Value:          []byte{5, 6, 7},
				Headers: []*RecordHeader{{
					Key:   []byte{8, 9, 10},
					Value: []byte{11, 12},
				}},
			}},
		},
		encoded: []byte{
			0, 0, 0, 0, 0, 0, 0, 0, // First Offset
			0, 0, 0, 89, // Length
			0, 0, 0, 0, // Partition Leader Epoch
			2,                // Version
			129, 238, 43, 82, // CRC
			0, 3, // Attributes
			0, 0, 0, 0, // Last Offset Delta
			0, 0, 0, 0, 0, 0, 0, 10, // First Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // Max Timestamp
			0, 0, 0, 0, 0, 0, 0, 0, // Producer ID
			0, 0, // Producer Epoch
			0, 0, 0, 0, // First Sequence
			0, 0, 0, 1, // Number of Records
			4, 34, 77, 24, 100, 112, 185, 21, 0, 0, 128, 40, 0, 10, 0, 8, 1, 2, 3, 4, 6, 5, 6, 7, 2,
			6, 8, 9, 10, 4, 11, 12, 0, 0, 0, 0, 12, 59, 239, 146,
		},
	},
}

func isOldGo(t *testing.T) bool {
	v := strings.Split(runtime.Version()[2:], ".")
	if len(v) < 2 {
		t.Logf("Can't parse version: %s", runtime.Version())
		return false
	}
	maj, err := strconv.Atoi(v[0])
	if err != nil {
		t.Logf("Can't parse version: %s", runtime.Version())
		return false
	}
	min, err := strconv.Atoi(v[1])
	if err != nil {
		t.Logf("Can't parse version: %s", runtime.Version())
		return false
	}
	return maj < 1 || (maj == 1 && min < 8)
}

func TestRecordBatchEncoding(t *testing.T) {
	for _, tc := range recordBatchTestCases {
		if tc.oldGoEncoded != nil && isOldGo(t) {
			testEncodable(t, tc.name, &tc.batch, tc.oldGoEncoded)
		} else {
			testEncodable(t, tc.name, &tc.batch, tc.encoded)
		}
	}
}

func TestRecordBatchDecoding(t *testing.T) {
	for _, tc := range recordBatchTestCases {
		batch := RecordBatch{}
		testDecodable(t, tc.name, &batch, tc.encoded)
		for _, r := range batch.Records {
			r.length = varintLengthField{}
		}
		for _, r := range tc.batch.Records {
			r.length = varintLengthField{}
		}
		if !reflect.DeepEqual(batch, tc.batch) {
			t.Errorf(spew.Sprintf("invalid decode of %s\ngot %+v\nwanted %+v", tc.name, batch, tc.batch))
		}
	}
}
