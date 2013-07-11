package kafka

type produceResponse struct {
}

func (pr *produceResponse) decode(pd packetDecoder) (err error) {
	return nil
}
