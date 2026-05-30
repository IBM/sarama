package sarama

type AddPartitionsToTxnTransaction struct {
	TransactionalID string
	ProducerID      int64
	ProducerEpoch   int16
	VerifyOnly      bool // v4
	TopicPartitions map[string][]int32
}

// AddPartitionsToTxnRequest is a add partition request
type AddPartitionsToTxnRequest struct {
	Version         int16
	TransactionalID string
	ProducerID      int64
	ProducerEpoch   int16
	TopicPartitions map[string][]int32
	Transactions    []AddPartitionsToTxnTransaction // v4
}

func (a *AddPartitionsToTxnRequest) setVersion(v int16) {
	a.Version = v
}

func (a *AddPartitionsToTxnRequest) encode(pe packetEncoder) error {
	if a.Version >= 4 {
		txns := a.Transactions
		if txns == nil {
			txns = []AddPartitionsToTxnTransaction{{
				TransactionalID: a.TransactionalID,
				ProducerID:      a.ProducerID,
				ProducerEpoch:   a.ProducerEpoch,
				TopicPartitions: a.TopicPartitions,
			}}
		}
		if err := pe.putArrayLength(len(txns)); err != nil {
			return err
		}
		for i := range txns {
			if err := encodeAddPartitionsToTxnTransaction(pe, &txns[i]); err != nil {
				return err
			}
		}
		pe.putEmptyTaggedFieldArray()
		return nil
	}

	if err := pe.putString(a.TransactionalID); err != nil {
		return err
	}
	pe.putInt64(a.ProducerID)
	pe.putInt16(a.ProducerEpoch)

	if err := pe.putArrayLength(len(a.TopicPartitions)); err != nil {
		return err
	}
	for topic, partitions := range a.TopicPartitions {
		if err := pe.putString(topic); err != nil {
			return err
		}
		if err := pe.putInt32Array(partitions); err != nil {
			return err
		}
		if a.isFlexible() {
			pe.putEmptyTaggedFieldArray()
		}
	}

	if a.isFlexible() {
		pe.putEmptyTaggedFieldArray()
	}
	return nil
}

func encodeAddPartitionsToTxnTransaction(pe packetEncoder, t *AddPartitionsToTxnTransaction) error {
	if err := pe.putString(t.TransactionalID); err != nil {
		return err
	}
	pe.putInt64(t.ProducerID)
	pe.putInt16(t.ProducerEpoch)
	pe.putBool(t.VerifyOnly)

	if err := pe.putArrayLength(len(t.TopicPartitions)); err != nil {
		return err
	}
	for topic, partitions := range t.TopicPartitions {
		if err := pe.putString(topic); err != nil {
			return err
		}
		if err := pe.putInt32Array(partitions); err != nil {
			return err
		}
		pe.putEmptyTaggedFieldArray()
	}
	pe.putEmptyTaggedFieldArray()
	return nil
}

func (a *AddPartitionsToTxnRequest) decode(pd packetDecoder, version int16) (err error) {
	a.Version = version

	if a.Version >= 4 {
		n, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		a.Transactions = make([]AddPartitionsToTxnTransaction, n)
		for i := 0; i < n; i++ {
			if err := decodeAddPartitionsToTxnTransaction(pd, &a.Transactions[i]); err != nil {
				return err
			}
		}
		_, err = pd.getEmptyTaggedFieldArray()
		return err
	}

	if a.TransactionalID, err = pd.getString(); err != nil {
		return err
	}
	if a.ProducerID, err = pd.getInt64(); err != nil {
		return err
	}
	if a.ProducerEpoch, err = pd.getInt16(); err != nil {
		return err
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	a.TopicPartitions = make(map[string][]int32, n)
	for i := 0; i < n; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}

		partitions, err := pd.getInt32Array()
		if err != nil {
			return err
		}

		a.TopicPartitions[topic] = partitions
		if a.isFlexible() {
			if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
				return err
			}
		}
	}

	if a.isFlexible() {
		if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}
	return nil
}

func decodeAddPartitionsToTxnTransaction(pd packetDecoder, t *AddPartitionsToTxnTransaction) (err error) {
	if t.TransactionalID, err = pd.getString(); err != nil {
		return err
	}
	if t.ProducerID, err = pd.getInt64(); err != nil {
		return err
	}
	if t.ProducerEpoch, err = pd.getInt16(); err != nil {
		return err
	}
	if t.VerifyOnly, err = pd.getBool(); err != nil {
		return err
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	t.TopicPartitions = make(map[string][]int32, n)
	for i := 0; i < n; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitions, err := pd.getInt32Array()
		if err != nil {
			return err
		}
		t.TopicPartitions[topic] = partitions
		if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}
	_, err = pd.getEmptyTaggedFieldArray()
	return err
}

func (a *AddPartitionsToTxnRequest) key() int16 {
	return apiKeyAddPartitionsToTxn
}

func (a *AddPartitionsToTxnRequest) version() int16 {
	return a.Version
}

func (a *AddPartitionsToTxnRequest) headerVersion() int16 {
	if a.isFlexible() {
		return 2
	}
	return 1
}

func (a *AddPartitionsToTxnRequest) isValidVersion() bool {
	return a.Version >= 0 && a.Version <= 5
}

func (a *AddPartitionsToTxnRequest) isFlexible() bool {
	return a.isFlexibleVersion(a.Version)
}

func (a *AddPartitionsToTxnRequest) isFlexibleVersion(version int16) bool {
	return version >= 3
}

func (a *AddPartitionsToTxnRequest) requiredVersion() KafkaVersion {
	switch a.Version {
	case 5:
		return V3_8_0_0
	case 4:
		return V3_6_0_0
	case 3:
		return V2_8_0_0
	case 2:
		return V2_7_0_0
	case 1:
		return V2_0_0_0
	default:
		return V0_11_0_0
	}
}
