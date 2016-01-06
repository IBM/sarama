package sarama

type ProtocolMetadata struct {
	Version      int16
	Subscription []string
	UserData     []byte
}

func (r *ProtocolMetadata) encode(pe packetEncoder) (err error) {
	pe.putInt16(int16(r.Version))

	if err := pe.putArrayLength(len(r.Subscription)); err != nil {
		return err
	}

	for _, sub := range r.Subscription {
		if err := pe.putString(sub); err != nil {
			return err
		}
	}

	if err := pe.putBytes(r.UserData); err != nil {
		return err
	}

	return nil
}

func (r *ProtocolMetadata) decode(pd packetDecoder) (err error) {
	if version, err := pd.getInt16(); err != nil {
		return err
	} else {
		r.Version = version
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	r.Subscription = make([]string, n)
	for i := 0; i < n; i++ {
		if subscription, err := pd.getString(); err != nil {
			return err
		} else {
			r.Subscription[i] = subscription
		}
	}

	if userData, err := pd.getBytes(); err != nil {
		return err
	} else {
		r.UserData = userData
	}

	return nil
}
