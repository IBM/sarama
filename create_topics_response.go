package sarama

type TopicErrorCode struct {
	Topic     string
	ErrorCode int16
}

type CreateTopicsResponse struct {
	TopicErrorCodes []*TopicErrorCode
}

func (c *CreateTopicsResponse) encode(e packetEncoder) error {
	if err := e.putArrayLength(len(c.TopicErrorCodes)); err != nil {
		return err
	}
	for _, t := range c.TopicErrorCodes {
		if err := e.putString(t.Topic); err != nil {
			return err
		}
		e.putInt16(t.ErrorCode)
	}
	return nil
}

func (c *CreateTopicsResponse) decode(d packetDecoder, version int16) error {
	topicCount, err := d.getArrayLength()
	if err != nil {
		return err
	}
	c.TopicErrorCodes = make([]*TopicErrorCode, topicCount)
	if topicCount > 0 {
		for i := 0; i < topicCount; i++ {
			topic, err := d.getString()
			if err != nil {
				return err
			}
			errorCode, err := d.getInt16()
			if err != nil {
				return err
			}
			c.TopicErrorCodes[i] = &TopicErrorCode{
				Topic:     topic,
				ErrorCode: errorCode,
			}
		}
	}
	return nil
}
