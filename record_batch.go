package sarama

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"

	"github.com/eapache/go-xerial-snappy"
	"github.com/pierrec/lz4"
)

const recordBatchOverhead = 49

type RecordBatch struct {
	FirstOffset           int64
	PartitionLeaderEpoch  int32
	Version               int8
	Codec                 CompressionCodec
	Control               bool
	LastOffsetDelta       int32
	FirstTimestamp        int64
	MaxTimestamp          int64
	ProducerID            int64
	ProducerEpoch         int16
	FirstSequence         int32
	Records               []*Record
	PartialTrailingRecord bool

	compressedRecords []byte
	recordsLen        int
}

func (b *RecordBatch) encode(pe packetEncoder) error {
	if b.Version != 2 {
		return PacketEncodingError{fmt.Sprintf("unsupported compression codec (%d)", b.Codec)}
	}
	pe.putInt64(b.FirstOffset)
	pe.push(&lengthField{})
	pe.putInt32(b.PartitionLeaderEpoch)
	pe.putInt8(b.Version)
	pe.push(newCRC32Field(crcCastagnoli))
	pe.putInt16(b.computeAttributes())
	pe.putInt32(b.LastOffsetDelta)
	pe.putInt64(b.FirstTimestamp)
	pe.putInt64(b.MaxTimestamp)
	pe.putInt64(b.ProducerID)
	pe.putInt16(b.ProducerEpoch)
	pe.putInt32(b.FirstSequence)

	if err := pe.putArrayLength(len(b.Records)); err != nil {
		return err
	}

	if b.compressedRecords != nil {
		if err := pe.putRawBytes(b.compressedRecords); err != nil {
			return err
		}
		if err := pe.pop(); err != nil {
			return err
		}
		if err := pe.pop(); err != nil {
			return err
		}
		return nil
	}

	var re packetEncoder
	var raw []byte

	switch b.Codec {
	case CompressionNone:
		re = pe
	case CompressionGZIP, CompressionLZ4, CompressionSnappy:
		for _, r := range b.Records {
			l, err := r.getTotalLength()
			if err != nil {
				return err
			}
			b.recordsLen += l
		}

		raw = make([]byte, b.recordsLen)
		re = &realEncoder{raw: raw}
	default:
		return PacketEncodingError{fmt.Sprintf("unsupported compression codec (%d)", b.Codec)}
	}

	for _, r := range b.Records {
		if err := r.encode(re); err != nil {
			return err
		}
	}

	switch b.Codec {
	case CompressionGZIP:
		var buf bytes.Buffer
		writer := gzip.NewWriter(&buf)
		if _, err := writer.Write(raw); err != nil {
			return err
		}
		if err := writer.Close(); err != nil {
			return err
		}
		b.compressedRecords = buf.Bytes()
	case CompressionSnappy:
		b.compressedRecords = snappy.Encode(raw)
	case CompressionLZ4:
		var buf bytes.Buffer
		writer := lz4.NewWriter(&buf)
		if _, err := writer.Write(raw); err != nil {
			return err
		}
		if err := writer.Close(); err != nil {
			return err
		}
		b.compressedRecords = buf.Bytes()
	}
	if err := pe.putRawBytes(b.compressedRecords); err != nil {
		return err
	}

	if err := pe.pop(); err != nil {
		return err
	}
	if err := pe.pop(); err != nil {
		return err
	}

	return nil
}

func (b *RecordBatch) decode(pd packetDecoder) (err error) {
	if b.FirstOffset, err = pd.getInt64(); err != nil {
		return err
	}

	var batchLen int32
	if batchLen, err = pd.getInt32(); err != nil {
		return err
	}

	if b.PartitionLeaderEpoch, err = pd.getInt32(); err != nil {
		return err
	}

	if b.Version, err = pd.getInt8(); err != nil {
		return err
	}

	if err = pd.push(&crc32Field{polynomial: crcCastagnoli}); err != nil {
		return err
	}

	var attributes int16
	if attributes, err = pd.getInt16(); err != nil {
		return err
	}
	b.Codec = CompressionCodec(int8(attributes) & compressionCodecMask)
	b.Control = attributes&controlMask == controlMask

	if b.LastOffsetDelta, err = pd.getInt32(); err != nil {
		return err
	}

	if b.FirstTimestamp, err = pd.getInt64(); err != nil {
		return err
	}

	if b.MaxTimestamp, err = pd.getInt64(); err != nil {
		return err
	}

	if b.ProducerID, err = pd.getInt64(); err != nil {
		return err
	}

	if b.ProducerEpoch, err = pd.getInt16(); err != nil {
		return err
	}

	if b.FirstSequence, err = pd.getInt32(); err != nil {
		return err
	}

	numRecs, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if numRecs >= 0 {
		b.Records = make([]*Record, numRecs)
	}

	bufSize := int(batchLen) - recordBatchOverhead
	recBuffer, err := pd.getRawBytes(bufSize)
	if err != nil {
		return err
	}

	if err = pd.pop(); err != nil {
		return err
	}

	switch b.Codec {
	case CompressionNone:
	case CompressionGZIP:
		reader, err := gzip.NewReader(bytes.NewReader(recBuffer))
		if err != nil {
			return err
		}
		if recBuffer, err = ioutil.ReadAll(reader); err != nil {
			return err
		}
	case CompressionSnappy:
		if recBuffer, err = snappy.Decode(recBuffer); err != nil {
			return err
		}
	case CompressionLZ4:
		reader := lz4.NewReader(bytes.NewReader(recBuffer))
		if recBuffer, err = ioutil.ReadAll(reader); err != nil {
			return err
		}
	default:
		return PacketDecodingError{fmt.Sprintf("invalid compression specified (%d)", b.Codec)}
	}
	recPd := &realDecoder{raw: recBuffer}

	for i := 0; i < numRecs; i++ {
		rec := &Record{}
		if err = rec.decode(recPd); err != nil {
			if err == ErrInsufficientData {
				b.PartialTrailingRecord = true
				b.Records = nil
				return nil
			}
			return err
		}
		b.Records[i] = rec
	}

	return nil
}

func (b *RecordBatch) computeAttributes() int16 {
	attr := int16(b.Codec) & int16(compressionCodecMask)
	if b.Control {
		attr |= controlMask
	}
	return attr
}

func (b *RecordBatch) addRecord(r *Record) {
	b.Records = append(b.Records, r)
}
