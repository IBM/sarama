package sarama

//APIVersionsResponseBlock is an api version reponse block type
type APIVersionsResponseBlock struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

func (b *APIVersionsResponseBlock) encode(pe packetEncoder) error {
	pe.putInt16(b.APIKey)
	pe.putInt16(b.MinVersion)
	pe.putInt16(b.MaxVersion)
	return nil
}

func (b *APIVersionsResponseBlock) decode(pd packetDecoder) error {
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

//APIVersionsResponse is an api version response type
type APIVersionsResponse struct {
	Err         KError
	APIVersions []*APIVersionsResponseBlock
}

func (a *APIVersionsResponse) encode(pe packetEncoder) error {
	pe.putInt16(int16(a.Err))
	if err := pe.putArrayLength(len(a.APIVersions)); err != nil {
		return err
	}
	for _, apiVersion := range a.APIVersions {
		if err := apiVersion.encode(pe); err != nil {
			return err
		}
	}
	return nil
}

func (a *APIVersionsResponse) decode(pd packetDecoder, version int16) error {
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}

	a.Err = KError(kerr)

	numBlocks, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	a.APIVersions = make([]*APIVersionsResponseBlock, numBlocks)
	for i := 0; i < numBlocks; i++ {
		block := new(APIVersionsResponseBlock)
		if err := block.decode(pd); err != nil {
			return err
		}
		a.APIVersions[i] = block
	}

	return nil
}

func (a *APIVersionsResponse) key() int16 {
	return 18
}

func (a *APIVersionsResponse) version() int16 {
	return 0
}

func (a *APIVersionsResponse) requiredVersion() KafkaVersion {
	return V0_10_0_0
}
