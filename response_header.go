package sarama

import "math"

type responseHeader struct {
	length        int32
	correlationID int32
}

func (r *responseHeader) decode(pd packetDecoder) (err error) {
	r.length, err = pd.getInt32()
	if err != nil {
		return err
	}
	if r.length <= 4 || r.length > 2*math.MaxUint16 {
		return DecodingError
	}

	r.correlationID, err = pd.getInt32()
	return err
}
