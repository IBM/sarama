package sarama

//EndTxnRequest is a transaction end request type
type EndTxnRequest struct {
	TransactionalID   string
	ProducerID        int64
	ProducerEpoch     int16
	TransactionResult bool
}

func (e *EndTxnRequest) encode(pe packetEncoder) error {
	if err := pe.putString(e.TransactionalID); err != nil {
		return err
	}

	pe.putInt64(e.ProducerID)

	pe.putInt16(e.ProducerEpoch)

	pe.putBool(e.TransactionResult)

	return nil
}

func (e *EndTxnRequest) decode(pd packetDecoder, version int16) (err error) {
	if e.TransactionalID, err = pd.getString(); err != nil {
		return err
	}
	if e.ProducerID, err = pd.getInt64(); err != nil {
		return err
	}
	if e.ProducerEpoch, err = pd.getInt16(); err != nil {
		return err
	}
	if e.TransactionResult, err = pd.getBool(); err != nil {
		return err
	}
	return nil
}

func (e *EndTxnRequest) key() int16 {
	return 26
}

func (e *EndTxnRequest) version() int16 {
	return 0
}

func (e *EndTxnRequest) requiredVersion() KafkaVersion {
	return V0_11_0_0
}
