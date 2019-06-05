package sarama

import (
	"fmt"
	"time"
)

const recordBatchOverhead = 49

type recordsArray []*Record

func (e recordsArray) encode(pe packetEncoder) error {
	for _, r := range e {
		if err := r.encode(pe); err != nil {
			return err
		}
	}
	return nil
}

func (e recordsArray) decode(pd packetDecoder) error {
	for i := range e {
		rec := &Record{}
		if err := rec.decode(pd); err != nil {
			return err
		}
		e[i] = rec
	}
	return nil
}

//RecordBatch is a record in a batch
type RecordBatch struct {
	FirstOffset           int64
	PartitionLeaderEpoch  int32
	Version               int8
	Codec                 CompressionCodec
	CompressionLevel      int
	Control               bool
	LogAppendTime         bool
	LastOffsetDelta       int32
	FirstTimestamp        time.Time
	MaxTimestamp          time.Time
	ProducerID            int64
	ProducerEpoch         int16
	FirstSequence         int32
	Records               []*Record
	PartialTrailingRecord bool
	IsTransactional       bool

	compressedRecords []byte
	recordsLen        int // uncompressed records size
}

//LastOffset return last offset in record batch
func (r *RecordBatch) LastOffset() int64 {
	return r.FirstOffset + int64(r.LastOffsetDelta)
}

func (r *RecordBatch) encode(pe packetEncoder) error {
	if r.Version != 2 {
		return PacketEncodingError{fmt.Sprintf("unsupported compression codec (%d)", r.Codec)}
	}
	pe.putInt64(r.FirstOffset)
	pe.push(&lengthField{})
	pe.putInt32(r.PartitionLeaderEpoch)
	pe.putInt8(r.Version)
	pe.push(newCRC32Field(crcCastagnoli))
	pe.putInt16(r.computeAttributes())
	pe.putInt32(r.LastOffsetDelta)

	if err := (Timestamp{&r.FirstTimestamp}).encode(pe); err != nil {
		return err
	}

	if err := (Timestamp{&r.MaxTimestamp}).encode(pe); err != nil {
		return err
	}

	pe.putInt64(r.ProducerID)
	pe.putInt16(r.ProducerEpoch)
	pe.putInt32(r.FirstSequence)

	if err := pe.putArrayLength(len(r.Records)); err != nil {
		return err
	}

	if r.compressedRecords == nil {
		if err := r.encodeRecords(pe); err != nil {
			return err
		}
	}
	if err := pe.putRawBytes(r.compressedRecords); err != nil {
		return err
	}

	if err := pe.pop(); err != nil {
		return err
	}
	return pe.pop()
}

func (r *RecordBatch) decode(pd packetDecoder) (err error) {
	if r.FirstOffset, err = pd.getInt64(); err != nil {
		return err
	}

	batchLen, err := pd.getInt32()
	if err != nil {
		return err
	}

	if r.PartitionLeaderEpoch, err = pd.getInt32(); err != nil {
		return err
	}

	if r.Version, err = pd.getInt8(); err != nil {
		return err
	}

	if err = pd.push(&crc32Field{polynomial: crcCastagnoli}); err != nil {
		return err
	}

	attributes, err := pd.getInt16()
	if err != nil {
		return err
	}
	r.Codec = CompressionCodec(int8(attributes) & compressionCodecMask)
	r.Control = attributes&controlMask == controlMask
	r.LogAppendTime = attributes&timestampTypeMask == timestampTypeMask
	r.IsTransactional = attributes&isTransactionalMask == isTransactionalMask

	if r.LastOffsetDelta, err = pd.getInt32(); err != nil {
		return err
	}

	if err = (Timestamp{&r.FirstTimestamp}).decode(pd); err != nil {
		return err
	}

	if err = (Timestamp{&r.MaxTimestamp}).decode(pd); err != nil {
		return err
	}

	if r.ProducerID, err = pd.getInt64(); err != nil {
		return err
	}

	if r.ProducerEpoch, err = pd.getInt16(); err != nil {
		return err
	}

	if r.FirstSequence, err = pd.getInt32(); err != nil {
		return err
	}

	numRecs, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if numRecs >= 0 {
		r.Records = make([]*Record, numRecs)
	}

	bufSize := int(batchLen) - recordBatchOverhead
	recBuffer, err := pd.getRawBytes(bufSize)
	if err != nil {
		if err == ErrInsufficientData {
			r.PartialTrailingRecord = true
			r.Records = nil
			return nil
		}
		return err
	}

	if err = pd.pop(); err != nil {
		return err
	}

	recBuffer, err = decompress(r.Codec, recBuffer)
	if err != nil {
		return err
	}

	r.recordsLen = len(recBuffer)
	err = decode(recBuffer, recordsArray(r.Records))
	if err == ErrInsufficientData {
		r.PartialTrailingRecord = true
		r.Records = nil
		return nil
	}
	return err
}

func (r *RecordBatch) encodeRecords(pe packetEncoder) error {
	var raw []byte
	var err error
	if raw, err = encode(recordsArray(r.Records), pe.metricRegistry()); err != nil {
		return err
	}
	r.recordsLen = len(raw)

	r.compressedRecords, err = compress(r.Codec, r.CompressionLevel, raw)
	return err
}

func (r *RecordBatch) computeAttributes() int16 {
	attr := int16(r.Codec) & int16(compressionCodecMask)
	if r.Control {
		attr |= controlMask
	}
	if r.LogAppendTime {
		attr |= timestampTypeMask
	}
	if r.IsTransactional {
		attr |= isTransactionalMask
	}
	return attr
}

func (r *RecordBatch) addRecord(record *Record) {
	r.Records = append(r.Records, record)
}
