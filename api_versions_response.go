package sarama

type ApiVersionsResponseBlock struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

func (b *ApiVersionsResponseBlock) encode(pe packetEncoder) error {
	pe.putInt16(b.APIKey)
	pe.putInt16(b.MinVersion)
	pe.putInt16(b.MaxVersion)
	return nil
}

func (b *ApiVersionsResponseBlock) decode(pd packetDecoder) error {
	var err error

	if b.APIKey, err = pd.getInt16(); err != nil {
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

type ApiVersionsResponse struct {
	Err         KError
	APIVersions []*ApiVersionsResponseBlock
}

func (r *ApiVersionsResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(r.Err))
	if err := pe.putArrayLength(len(r.APIVersions)); err != nil {
		return err
	}
	for _, apiVersion := range r.APIVersions {
		if err := apiVersion.encode(pe); err != nil {
			return err
		}
	}
	return nil
}

func (r *ApiVersionsResponse) decode(pd packetDecoder, version int16) error {
	if kerr, err := pd.getInt16(); err != nil {
		return err
	} else {
		r.Err = KError(kerr)
	}

	numBlocks, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	r.APIVersions = make([]*ApiVersionsResponseBlock, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := new(ApiVersionsResponseBlock)
		if err := block.decode(pd); err != nil {
			return err
		}
		r.APIVersions[i] = block
	}

	return nil
}

func (r *ApiVersionsResponse) key() int16 {
	return 18
}

func (r *ApiVersionsResponse) version() int16 {
	return 0
}

func (r *ApiVersionsResponse) requiredVersion() KafkaVersion {
	return V0_10_0_0
}
