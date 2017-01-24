package sarama

type CreateTopicResponse struct {
	Topic string
	Err   KError
}

type CreateTopicsResponse struct {
	CreateTopicResponses []*CreateTopicResponse
}

func (ct *CreateTopicsResponse) decode(pd packetDecoder, version int16) error {
	responseCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	ct.CreateTopicResponses = make([]*CreateTopicResponse, responseCount)

	for i := range ct.CreateTopicResponses {
		ct.CreateTopicResponses[i] = new(CreateTopicResponse)

		topic, err := pd.getString()
		if err != nil {
			return err
		}
		ct.CreateTopicResponses[i].Topic = topic

		tmp, err := pd.getInt16()
		if err != nil {
			return err
		}
		ct.CreateTopicResponses[i].Err = KError(tmp)
	}

	return nil
}

func (ct *CreateTopicsResponse) encode(pe packetEncoder) error {
	if err := pe.putArrayLength(len(ct.CreateTopicResponses)); err != nil {
		return err
	}

	for i := range ct.CreateTopicResponses {
		if err := pe.putString(ct.CreateTopicResponses[i].Topic); err != nil {
			return err
		}

		pe.putInt16(int16(ct.CreateTopicResponses[i].Err))
	}

	return nil
}
