package sarama

type APIVersionsResponseBlock struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

func (b *APIVersionsResponseBlock) encode(pe packetEncoder) error {
	pe.putInt16(b.ApiKey)
	pe.putInt16(b.MinVersion)
	pe.putInt16(b.MaxVersion)
	return nil
}

func (b *APIVersionsResponseBlock) decode(pd packetDecoder) error {
	var err error

	if b.ApiKey, err = pd.getInt16(); err != nil {
		return err
	}

	if b.MinVersion, err = pd.getInt16(); err != nil {
		return err
	}

	if b.MaxVersion, err = pd.getInt16(); err != nil {
		return err
	}

	return nil
}

type APIVersionsResponse struct {
	Err         KError
	ApiVersions []*APIVersionsResponseBlock
}

func (r *APIVersionsResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(r.Err))
	if err := pe.putArrayLength(len(r.ApiVersions)); err != nil {
		return err
	}
	for _, apiVersion := range r.ApiVersions {
		if err := apiVersion.encode(pe); err != nil {
			return err
		}
	}
	return nil
}

func (r *APIVersionsResponse) decode(pd packetDecoder, version int16) error {
	if kerr, err := pd.getInt16(); err != nil {
		return err
	} else {
		r.Err = KError(kerr)
	}

	numBlocks, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	r.ApiVersions = make([]*APIVersionsResponseBlock, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := new(APIVersionsResponseBlock)
		if err := block.decode(pd); err != nil {
			return err
		}
		r.ApiVersions[i] = block
	}

	return nil
}

func (r *APIVersionsResponse) key() int16 {
	return 18
}

func (r *APIVersionsResponse) version() int16 {
	return 0
}

func (r *APIVersionsResponse) requiredVersion() KafkaVersion {
	return V0_10_0_0
}
