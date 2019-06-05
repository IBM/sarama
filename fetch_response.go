package sarama

import (
	"sort"
	"time"
)

//AbortedTransaction is an aborted txn type
type AbortedTransaction struct {
	ProducerID  int64
	FirstOffset int64
}

func (a *AbortedTransaction) decode(pd packetDecoder) (err error) {
	if a.ProducerID, err = pd.getInt64(); err != nil {
		return err
	}

	if a.FirstOffset, err = pd.getInt64(); err != nil {
		return err
	}

	return nil
}

func (a *AbortedTransaction) encode(pe packetEncoder) (err error) {
	pe.putInt64(a.ProducerID)
	pe.putInt64(a.FirstOffset)

	return nil
}

//FetchResponseBlock is a fetched response block
type FetchResponseBlock struct {
	Err                 KError
	HighWaterMarkOffset int64
	LastStableOffset    int64
	AbortedTransactions []*AbortedTransaction
	RecordsSet          []*Records
	Partial             bool
}

func (b *FetchResponseBlock) decode(pd packetDecoder, version int16) (err error) {
	tmp, err := pd.getInt16()
	if err != nil {
		return err
	}
	b.Err = KError(tmp)

	b.HighWaterMarkOffset, err = pd.getInt64()
	if err != nil {
		return err
	}

	if version >= 4 {
		b.LastStableOffset, err = pd.getInt64()
		if err != nil {
			return err
		}

		numTransact, err := pd.getArrayLength()
		if err != nil {
			return err
		}

		if numTransact >= 0 {
			b.AbortedTransactions = make([]*AbortedTransaction, numTransact)
		}

		for i := 0; i < numTransact; i++ {
			transact := new(AbortedTransaction)
			if err = transact.decode(pd); err != nil {
				return err
			}
			b.AbortedTransactions[i] = transact
		}
	}

	recordsSize, err := pd.getInt32()
	if err != nil {
		return err
	}

	recordsDecoder, err := pd.getSubset(int(recordsSize))
	if err != nil {
		return err
	}

	b.RecordsSet = []*Records{}

	for recordsDecoder.remaining() > 0 {
		records := &Records{}
		if err := records.decode(recordsDecoder); err != nil {
			// If we have at least one decoded records, this is not an error
			if err == ErrInsufficientData {
				if len(b.RecordsSet) == 0 {
					b.Partial = true
				}
				break
			}
			return err
		}

		partial, err := records.isPartial()
		if err != nil {
			return err
		}

		n, err := records.numRecords()
		if err != nil {
			return err
		}

		if n > 0 || (partial && len(b.RecordsSet) == 0) {
			b.RecordsSet = append(b.RecordsSet, records)

		}

		overflow, err := records.isOverflow()
		if err != nil {
			return err
		}

		if partial || overflow {
			break
		}
	}

	return nil
}

func (b *FetchResponseBlock) numRecords() (int, error) {
	sum := 0

	for _, records := range b.RecordsSet {
		count, err := records.numRecords()
		if err != nil {
			return 0, err
		}

		sum += count
	}

	return sum, nil
}

func (b *FetchResponseBlock) isPartial() (bool, error) {
	if b.Partial {
		return true, nil
	}

	if len(b.RecordsSet) == 1 {
		return b.RecordsSet[0].isPartial()
	}

	return false, nil
}

func (b *FetchResponseBlock) encode(pe packetEncoder, version int16) (err error) {
	pe.putInt16(int16(b.Err))

	pe.putInt64(b.HighWaterMarkOffset)

	if version >= 4 {
		pe.putInt64(b.LastStableOffset)

		if err = pe.putArrayLength(len(b.AbortedTransactions)); err != nil {
			return err
		}
		for _, transact := range b.AbortedTransactions {
			if err = transact.encode(pe); err != nil {
				return err
			}
		}
	}

	pe.push(&lengthField{})
	for _, records := range b.RecordsSet {
		err = records.encode(pe)
		if err != nil {
			return err
		}
	}
	return pe.pop()
}

func (b *FetchResponseBlock) getAbortedTransactions() []*AbortedTransaction {
	// I can't find any doc that guarantee the field `fetchResponse.AbortedTransactions` is ordered
	// plus Java implementation use a PriorityQueue based on `FirstOffset`. I guess we have to order it ourself
	at := b.AbortedTransactions
	sort.Slice(
		at,
		func(i, j int) bool { return at[i].FirstOffset < at[j].FirstOffset },
	)
	return at
}

//FetchResponse is fetch response type
type FetchResponse struct {
	Blocks        map[string]map[int32]*FetchResponseBlock
	ThrottleTime  time.Duration
	Version       int16 // v1 requires 0.9+, v2 requires 0.10+
	LogAppendTime bool
	Timestamp     time.Time
}

func (f *FetchResponse) decode(pd packetDecoder, version int16) (err error) {
	f.Version = version

	if f.Version >= 1 {
		throttle, err := pd.getInt32()
		if err != nil {
			return err
		}
		f.ThrottleTime = time.Duration(throttle) * time.Millisecond
	}

	numTopics, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	f.Blocks = make(map[string]map[int32]*FetchResponseBlock, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.getString()
		if err != nil {
			return err
		}

		numBlocks, err := pd.getArrayLength()
		if err != nil {
			return err
		}

		f.Blocks[name] = make(map[int32]*FetchResponseBlock, numBlocks)

		for j := 0; j < numBlocks; j++ {
			id, err := pd.getInt32()
			if err != nil {
				return err
			}

			block := new(FetchResponseBlock)
			err = block.decode(pd, version)
			if err != nil {
				return err
			}
			f.Blocks[name][id] = block
		}
	}

	return nil
}

func (f *FetchResponse) encode(pe packetEncoder) (err error) {
	if f.Version >= 1 {
		pe.putInt32(int32(f.ThrottleTime / time.Millisecond))
	}

	err = pe.putArrayLength(len(f.Blocks))
	if err != nil {
		return err
	}

	for topic, partitions := range f.Blocks {
		err = pe.putString(topic)
		if err != nil {
			return err
		}

		err = pe.putArrayLength(len(partitions))
		if err != nil {
			return err
		}

		for id, block := range partitions {
			pe.putInt32(id)
			err = block.encode(pe, f.Version)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func (f *FetchResponse) key() int16 {
	return 1
}

func (f *FetchResponse) version() int16 {
	return f.Version
}

func (f *FetchResponse) requiredVersion() KafkaVersion {
	switch f.Version {
	case 1:
		return V0_9_0_0
	case 2:
		return V0_10_0_0
	case 3:
		return V0_10_1_0
	case 4:
		return V0_11_0_0
	default:
		return MinVersion
	}
}

//GetBlock returns a fetch response block
func (f *FetchResponse) GetBlock(topic string, partition int32) *FetchResponseBlock {
	if f.Blocks == nil {
		return nil
	}

	if f.Blocks[topic] == nil {
		return nil
	}

	return f.Blocks[topic][partition]
}

//AddError adds an error to fetch response
func (f *FetchResponse) AddError(topic string, partition int32, err KError) {
	if f.Blocks == nil {
		f.Blocks = make(map[string]map[int32]*FetchResponseBlock)
	}
	partitions, ok := f.Blocks[topic]
	if !ok {
		partitions = make(map[int32]*FetchResponseBlock)
		f.Blocks[topic] = partitions
	}
	frb, ok := partitions[partition]
	if !ok {
		frb = new(FetchResponseBlock)
		partitions[partition] = frb
	}
	frb.Err = err
}

func (f *FetchResponse) getOrCreateBlock(topic string, partition int32) *FetchResponseBlock {
	if f.Blocks == nil {
		f.Blocks = make(map[string]map[int32]*FetchResponseBlock)
	}
	partitions, ok := f.Blocks[topic]
	if !ok {
		partitions = make(map[int32]*FetchResponseBlock)
		f.Blocks[topic] = partitions
	}
	frb, ok := partitions[partition]
	if !ok {
		frb = new(FetchResponseBlock)
		partitions[partition] = frb
	}

	return frb
}

func encodeKV(key, value Encoder) ([]byte, []byte) {
	var kb []byte
	var vb []byte
	if key != nil {
		kb, _ = key.Encode()
	}
	if value != nil {
		vb, _ = value.Encode()
	}

	return kb, vb
}

//AddMessageWithTimestamp adds a timestamp to message
func (f *FetchResponse) AddMessageWithTimestamp(topic string, partition int32, key, value Encoder, offset int64, timestamp time.Time, version int8) {
	frb := f.getOrCreateBlock(topic, partition)
	kb, vb := encodeKV(key, value)
	if f.LogAppendTime {
		timestamp = f.Timestamp
	}
	msg := &Message{Key: kb, Value: vb, LogAppendTime: f.LogAppendTime, Timestamp: timestamp, Version: version}
	msgBlock := &MessageBlock{Msg: msg, Offset: offset}
	if len(frb.RecordsSet) == 0 {
		records := newLegacyRecords(&MessageSet{})
		frb.RecordsSet = []*Records{&records}
	}
	set := frb.RecordsSet[0].MsgSet
	set.Messages = append(set.Messages, msgBlock)
}

//AddRecordWithTimestamp adds a timestamp to record
func (f *FetchResponse) AddRecordWithTimestamp(topic string, partition int32, key, value Encoder, offset int64, timestamp time.Time) {
	frb := f.getOrCreateBlock(topic, partition)
	kb, vb := encodeKV(key, value)
	if len(frb.RecordsSet) == 0 {
		records := newDefaultRecords(&RecordBatch{Version: 2, LogAppendTime: f.LogAppendTime, FirstTimestamp: timestamp, MaxTimestamp: f.Timestamp})
		frb.RecordsSet = []*Records{&records}
	}
	batch := frb.RecordsSet[0].RecordBatch
	rec := &Record{Key: kb, Value: vb, OffsetDelta: offset, TimestampDelta: timestamp.Sub(batch.FirstTimestamp)}
	batch.addRecord(rec)
}

// AddRecordBatchWithTimestamp is similar to AddRecordWithTimestamp
// But instead of appending 1 record to a batch, it append a new batch containing 1 record to the fetchResponse
// Since transaction are handled on batch level (the whole batch is either committed or aborted), use this to test transactions
func (f *FetchResponse) AddRecordBatchWithTimestamp(topic string, partition int32, key, value Encoder, offset int64, producerID int64, isTransactional bool, timestamp time.Time) {
	frb := f.getOrCreateBlock(topic, partition)
	kb, vb := encodeKV(key, value)

	records := newDefaultRecords(&RecordBatch{Version: 2, LogAppendTime: f.LogAppendTime, FirstTimestamp: timestamp, MaxTimestamp: f.Timestamp})
	batch := &RecordBatch{
		Version:         2,
		LogAppendTime:   f.LogAppendTime,
		FirstTimestamp:  timestamp,
		MaxTimestamp:    f.Timestamp,
		FirstOffset:     offset,
		LastOffsetDelta: 0,
		ProducerID:      producerID,
		IsTransactional: isTransactional,
	}
	rec := &Record{Key: kb, Value: vb, OffsetDelta: 0, TimestampDelta: timestamp.Sub(batch.FirstTimestamp)}
	batch.addRecord(rec)
	records.RecordBatch = batch

	frb.RecordsSet = append(frb.RecordsSet, &records)
}

//AddControlRecordWithTimestamp adds a control record with timestamp
func (f *FetchResponse) AddControlRecordWithTimestamp(topic string, partition int32, offset int64, producerID int64, recordType ControlRecordType, timestamp time.Time) {
	frb := f.getOrCreateBlock(topic, partition)

	// batch
	batch := &RecordBatch{
		Version:         2,
		LogAppendTime:   f.LogAppendTime,
		FirstTimestamp:  timestamp,
		MaxTimestamp:    f.Timestamp,
		FirstOffset:     offset,
		LastOffsetDelta: 0,
		ProducerID:      producerID,
		IsTransactional: true,
		Control:         true,
	}

	// records
	records := newDefaultRecords(nil)
	records.RecordBatch = batch

	// record
	crAbort := ControlRecord{
		Version: 0,
		Type:    recordType,
	}
	crKey := &realEncoder{raw: make([]byte, 4)}
	crValue := &realEncoder{raw: make([]byte, 6)}
	crAbort.encode(crKey, crValue)
	rec := &Record{Key: ByteEncoder(crKey.raw), Value: ByteEncoder(crValue.raw), OffsetDelta: 0, TimestampDelta: timestamp.Sub(batch.FirstTimestamp)}
	batch.addRecord(rec)

	frb.RecordsSet = append(frb.RecordsSet, &records)
}

//AddMessage is a wrapper for AddMessageWithTimestamp
func (f *FetchResponse) AddMessage(topic string, partition int32, key, value Encoder, offset int64) {
	f.AddMessageWithTimestamp(topic, partition, key, value, offset, time.Time{}, 0)
}

//AddRecord is a wrapper for AddRecordWithTimestamp
func (f *FetchResponse) AddRecord(topic string, partition int32, key, value Encoder, offset int64) {
	f.AddRecordWithTimestamp(topic, partition, key, value, offset, time.Time{})
}

//AddRecordBatch is a wrapper for AddRecordBatchWithTimestamp
func (f *FetchResponse) AddRecordBatch(topic string, partition int32, key, value Encoder, offset int64, producerID int64, isTransactional bool) {
	f.AddRecordBatchWithTimestamp(topic, partition, key, value, offset, producerID, isTransactional, time.Time{})
}

//AddControlRecord is a wrapper type for AddControlRecordWithTimestamp
func (f *FetchResponse) AddControlRecord(topic string, partition int32, offset int64, producerID int64, recordType ControlRecordType) {
	// define controlRecord key and value
	f.AddControlRecordWithTimestamp(topic, partition, offset, producerID, recordType, time.Time{})
}

//SetLastOffsetDelta sets last offset delta
func (f *FetchResponse) SetLastOffsetDelta(topic string, partition int32, offset int32) {
	frb := f.getOrCreateBlock(topic, partition)
	if len(frb.RecordsSet) == 0 {
		records := newDefaultRecords(&RecordBatch{Version: 2})
		frb.RecordsSet = []*Records{&records}
	}
	batch := frb.RecordsSet[0].RecordBatch
	batch.LastOffsetDelta = offset
}

//SetLastStableOffset sets last stable offset
func (f *FetchResponse) SetLastStableOffset(topic string, partition int32, offset int64) {
	frb := f.getOrCreateBlock(topic, partition)
	frb.LastStableOffset = offset
}
