package protocol

import "math"
import enc "sarama/encoding"

type responseHeader struct {
	length         int32
	correlation_id int32
}

func (r *responseHeader) Decode(pd enc.PacketDecoder) (err error) {
	r.length, err = pd.GetInt32()
	if err != nil {
		return err
	}
	if r.length <= 4 || r.length > 2*math.MaxUint16 {
		return enc.DecodingError("Malformed length field.")
	}

	r.correlation_id, err = pd.GetInt32()
	return err
}
