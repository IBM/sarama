//go:build !functional

package sarama

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
)

func recordBatchTestCases() []struct {
	name         string
	batch        RecordBatch
	encoded      []byte
	oldGoEncoded []byte
} {
	return []struct {
		name         string
		batch        RecordBatch
		encoded      []byte
		oldGoEncoded []byte // used in case of gzipped content for go versions prior to 1.8
	}{
		{
			name: "empty record",
			batch: RecordBatch{
				Version:        2,
				FirstTimestamp: time.Unix(0, 0),
				MaxTimestamp:   time.Unix(0, 0),
				Records:        []*Record{},
			},
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
			name: "control batch",
			batch: RecordBatch{
				Version:        2,
				Control:        true,
				FirstTimestamp: time.Unix(0, 0),
				MaxTimestamp:   time.Unix(0, 0),
				Records:        []*Record{},
			},
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
				Version:         2,
				FirstTimestamp:  time.Unix(1479847795, 0),
				MaxTimestamp:    time.Unix(0, 0),
				LastOffsetDelta: 0,
				Records: []*Record{{
					TimestampDelta: 5 * time.Millisecond,
					Key:            []byte{1, 2, 3, 4},
					Value:          []byte{5, 6, 7},
					Headers: []*RecordHeader{{
						Key:   []byte{8, 9, 10},
						Value: []byte{11, 12},
					}},
				}},
				recordsLen: 21,
			},
			encoded: []byte{
				0, 0, 0, 0, 0, 0, 0, 0, // First Offset
				0, 0, 0, 70, // Length
				0, 0, 0, 0, // Partition Leader Epoch
				2,                // Version
				84, 121, 97, 253, // CRC
				0, 0, // Attributes
				0, 0, 0, 0, // Last Offset Delta
				0, 0, 1, 88, 141, 205, 89, 56, // First Timestamp
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
				Version:          2,
				Codec:            CompressionGZIP,
				CompressionLevel: CompressionLevelDefault,
				FirstTimestamp:   time.Unix(1479847795, 0),
				MaxTimestamp:     time.Unix(0, 0),
				LastOffsetDelta:  0,
				Records: []*Record{{
					TimestampDelta: 5 * time.Millisecond,
					Key:            []byte{1, 2, 3, 4},
					Value:          []byte{5, 6, 7},
					Headers: []*RecordHeader{{
						Key:   []byte{8, 9, 10},
						Value: []byte{11, 12},
					}},
				}},
				recordsLen: 21,
			},
			encoded: []byte{
				0, 0, 0, 0, 0, 0, 0, 0, // First Offset
				0, 0, 0, 95, // Length
				0, 0, 0, 0, // Partition Leader Epoch
				2,                 // Version
				231, 74, 206, 165, // CRC
				0, 1, // Attributes
				0, 0, 0, 0, // Last Offset Delta
				0, 0, 1, 88, 141, 205, 89, 56, // First Timestamp
				0, 0, 0, 0, 0, 0, 0, 0, // Max Timestamp
				0, 0, 0, 0, 0, 0, 0, 0, // Producer ID
				0, 0, // Producer Epoch
				0, 0, 0, 0, // First Sequence
				0, 0, 0, 1, // Number of Records
				31, 139, 8, 0, 0, 9, 110, 136, 0, 255, 0, 21, 0, 234, 255, 40, 0, 10, 0, 8, 1, 2,
				3, 4, 6, 5, 6, 7, 2, 6, 8, 9, 10, 4, 11, 12, 3, 0, 173, 201, 88, 103, 21, 0, 0, 0,
			},
		},
		{
			name: "snappy compressed record",
			batch: RecordBatch{
				Version:         2,
				Codec:           CompressionSnappy,
				FirstTimestamp:  time.Unix(1479847795, 0),
				MaxTimestamp:    time.Unix(0, 0),
				LastOffsetDelta: 0,
				Records: []*Record{{
					TimestampDelta: 5 * time.Millisecond,
					Key:            []byte{1, 2, 3, 4},
					Value:          []byte{5, 6, 7},
					Headers: []*RecordHeader{{
						Key:   []byte{8, 9, 10},
						Value: []byte{11, 12},
					}},
				}},
				recordsLen: 21,
			},
			encoded: []byte{
				0, 0, 0, 0, 0, 0, 0, 0, // First Offset
				0, 0, 0, 92, // Length
				0, 0, 0, 0, // Partition Leader Epoch
				2,                 // Version
				133, 100, 178, 36, // CRC
				0, 2, // Attributes
				0, 0, 0, 0, // Last Offset Delta
				0, 0, 1, 88, 141, 205, 89, 56, // First Timestamp
				0, 0, 0, 0, 0, 0, 0, 0, // Max Timestamp
				0, 0, 0, 0, 0, 0, 0, 0, // Producer ID
				0, 0, // Producer Epoch
				0, 0, 0, 0, // First Sequence
				0, 0, 0, 1, // Number of Records
				// Xerial framing header + compressed data
				130, 83, 78, 65, 80, 80, 89, 0, // SNAPPY magic
				0, 0, 0, 1, // min version
				0, 0, 0, 1, // default version
				0, 0, 0, 23, // compressed length
				21, 80, 40, 0, 10, 0, 8, 1, 2, 3, 4, 6, 5, 6, 7, 2, 6, 8, 9, 10, 4, 11, 12, // compressed data
			},
		},
		{
			name: "lz4 compressed record",
			batch: RecordBatch{
				Version:         2,
				Codec:           CompressionLZ4,
				FirstTimestamp:  time.Unix(1479847795, 0),
				MaxTimestamp:    time.Unix(0, 0),
				LastOffsetDelta: 0,
				Records: []*Record{{
					TimestampDelta: 5 * time.Millisecond,
					Key:            []byte{1, 2, 3, 4},
					Value:          []byte{5, 6, 7},
					Headers: []*RecordHeader{{
						Key:   []byte{8, 9, 10},
						Value: []byte{11, 12},
					}},
				}},
				recordsLen: 21,
			},
			encoded: []byte{
				0, 0, 0, 0, 0, 0, 0, 0, // First Offset
				0, 0, 0, 89, // Length
				0, 0, 0, 0, // Partition Leader Epoch
				2,                 // Version
				157, 18, 145, 248, // CRC
				0, 3, // Attributes
				0, 0, 0, 0, // Last Offset Delta
				0, 0, 1, 88, 141, 205, 89, 56, // First Timestamp
				0, 0, 0, 0, 0, 0, 0, 0, // Max Timestamp
				0, 0, 0, 0, 0, 0, 0, 0, // Producer ID
				0, 0, // Producer Epoch
				0, 0, 0, 0, // First Sequence
				0, 0, 0, 1, // Number of Records
				4, 34, 77, 24, 100, 64, 167, 21, 0, 0, 128, 40, 0, 10, 0, 8, 1, 2, 3, 4, 6, 5, 6, 7, 2,
				6, 8, 9, 10, 4, 11, 12, 0, 0, 0, 0, 12, 59, 239, 146,
			},
		},
		//
	}
}

func TestRecordBatchEncoding(t *testing.T) {
	for _, tc := range recordBatchTestCases() {
		testEncodable(t, tc.name, &tc.batch, tc.encoded)
	}
}

func TestRecordBatchDecoding(t *testing.T) {
	for _, tc := range recordBatchTestCases() {
		batch := RecordBatch{}
		testDecodable(t, tc.name, &batch, tc.encoded)
		for _, r := range batch.Records {
			r.length = varintLengthField{}
		}
		// The compression level is not restored on decoding. It is not needed
		// anyway. We only set it here to ensure that comparison succeeds.
		batch.CompressionLevel = tc.batch.CompressionLevel
		if !reflect.DeepEqual(batch, tc.batch) {
			t.Error(spew.Sprintf("invalid decode of %s\ngot %+v\nwanted %+v", tc.name, batch, tc.batch))
		}
	}
}

func TestRecordBatchLargeNumRecords(t *testing.T) {
	numOfRecords := 10 + (2 * math.MaxUint16)
	numofRecordsBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(numofRecordsBytes, uint32(numOfRecords))

	encodedBatch := []byte{
		0, 0, 0, 0, 0, 0, 0, 0, // First Offset
		0, 42, 0, 250, // Length
		0, 0, 0, 0, // Partition Leader Epoch
		2,                 // Version
		103, 68, 166, 213, // CRC
		0, 0, // Attributes
		0, 0, 0, 0, // Last Offset Delta
		0, 0, 1, 88, 141, 205, 89, 56, // First Timestamp
		0, 0, 0, 0, 0, 0, 0, 0, // Max Timestamp
		0, 0, 0, 0, 0, 0, 0, 0, // Producer ID
		0, 0, // Producer Epoch
		0, 0, 0, 0, // First Sequence
	}

	encodedBatch = append(encodedBatch, numofRecordsBytes...)

	for range numOfRecords {
		encodedBatch = append(encodedBatch, []byte{
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
		}...)
	}

	var batch RecordBatch
	err := decode(encodedBatch, &batch, nil)
	if err != nil {
		t.Fatal("received error while decoding record batch", err)
	}
}

func TestRecordBatchMoreRecordsThanCompressedBytes(t *testing.T) {
	// zstd squeezes 2000 small, similar records into fewer than 2000 bytes,
	// so the record count is larger than the bytes left on the wire.
	const numRecords = 2000
	batch := RecordBatch{
		Version:         2,
		Codec:           CompressionZSTD,
		LastOffsetDelta: numRecords - 1,
	}
	for i := range numRecords {
		batch.addRecord(&Record{OffsetDelta: int64(i), Value: []byte("hello")})
	}

	encoded, err := encode(&batch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= numRecords {
		t.Fatalf("expected the encoded batch to be smaller than %d bytes, got %d", numRecords, len(encoded))
	}

	var decoded RecordBatch
	if err := decode(encoded, &decoded, nil); err != nil {
		t.Fatal("received error while decoding record batch", err)
	}
	if decoded.PartialTrailingRecord {
		t.Fatal("complete record batch decoded as partial")
	}
	if len(decoded.Records) != numRecords {
		t.Fatalf("expected %d records, got %d", numRecords, len(decoded.Records))
	}
	for i, r := range decoded.Records {
		if r.OffsetDelta != int64(i) || string(r.Value) != "hello" {
			t.Fatalf("record %d decoded as offset delta %d value %q", i, r.OffsetDelta, r.Value)
		}
	}
}

func TestRecordBatchCompressedRecordCountDoesNotDriveAllocation(t *testing.T) {
	// a small gzip batch can claim far more records than it holds; it must
	// decode as a partial batch without allocating for the claimed count
	const valueSize = 1 << 20
	encoded, err := encode(&RecordBatch{
		Version: 2,
		Codec:   CompressionGZIP,
		Records: []*Record{{Value: make([]byte, valueSize)}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(encoded[57:], uint32(MaxResponseSize))
	binary.BigEndian.PutUint32(encoded[17:], crc32.Checksum(encoded[21:], crc32.MakeTable(crc32.Castagnoli)))

	var decoded RecordBatch
	if err := decode(encoded, &decoded, nil); err != nil {
		t.Fatal("received error while decoding record batch", err)
	}
	if !decoded.PartialTrailingRecord || decoded.Records != nil {
		t.Fatalf("expected a partial batch with no records, got partial=%v records=%d",
			decoded.PartialTrailingRecord, len(decoded.Records))
	}
}

func TestLazyRecordsAllocateAsTheyDecode(t *testing.T) {
	// one real record followed by nothing, but a claimed count of 1<<30
	raw, err := encode(recordsArray{{Value: []byte("hello")}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	records := &lazyRecords{n: 1 << 30}
	if err := decode(raw, records, nil); !errors.Is(err, ErrInsufficientData) {
		t.Fatalf("expected ErrInsufficientData, got %v", err)
	}
	if len(records.records) != 1 || cap(records.records) > recordsChunk {
		t.Fatalf("decoded %d records with capacity %d, want 1 record and capacity <= %d",
			len(records.records), cap(records.records), recordsChunk)
	}
}

func TestRecordBatchRecordCountLargerThanRecords(t *testing.T) {
	// an uncompressed batch claiming more records than its payload could
	// hold must not allocate for the claimed count
	encoded, err := encode(&RecordBatch{
		Version: 2,
		Records: []*Record{{Value: []byte{1}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// the record count sits at offset 57, the CRC at 17 covers offset 21 on
	binary.BigEndian.PutUint32(encoded[57:], uint32(MaxResponseSize))
	binary.BigEndian.PutUint32(encoded[17:], crc32.Checksum(encoded[21:], crc32.MakeTable(crc32.Castagnoli)))

	var decoded RecordBatch
	if err := decode(encoded, &decoded, nil); err != nil {
		t.Fatal("received error while decoding record batch", err)
	}
	if !decoded.PartialTrailingRecord || decoded.Records != nil {
		t.Fatalf("expected a partial batch with no records, got partial=%v records=%d",
			decoded.PartialTrailingRecord, len(decoded.Records))
	}
}
