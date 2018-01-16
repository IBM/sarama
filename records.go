package sarama

const (
	magicOffset = 16
	magicLength = 1
)

// Records implements a union type containing either a RecordBatch or a legacy MessageSet.
type Records struct {
	msgSet      *MessageSet
	recordBatch *RecordBatch
}

func (c *Records) numRecords() int {
	if c.msgSet != nil {
		return len(c.msgSet.Messages)
	}

	if c.recordBatch != nil {
		return len(c.recordBatch.Records)
	}

	return 0
}

func (c *Records) isPartial() bool {
	if c.msgSet != nil {
		return c.msgSet.PartialTrailingMessage
	}

	if c.recordBatch != nil {
		return c.recordBatch.PartialTrailingRecord
	}

	return false
}

func (c *Records) decode(pd packetDecoder) (err error) {
	magic, err := magicValue(pd)
	if err != nil {
		return err
	}

	if magic < 2 {
		c.msgSet = &MessageSet{}
		return c.msgSet.decode(pd)
	}

	c.recordBatch = &RecordBatch{}
	return c.recordBatch.decode(pd)
}

func (c *Records) encode(pe packetEncoder) (err error) {
	if c.msgSet != nil {
		return c.msgSet.encode(pe)
	}

	if c.recordBatch != nil {
		return c.recordBatch.encode(pe)
	}

	return nil
}

func magicValue(pd packetDecoder) (int8, error) {
	dec, err := pd.peek(magicOffset, magicLength)
	if err != nil {
		return 0, err
	}

	return dec.getInt8()
}
