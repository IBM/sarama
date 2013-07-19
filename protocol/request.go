package protocol

import enc "sarama/encoding"

type requestEncoder interface {
	enc.Encoder
	key() int16
	version() int16
}

type request struct {
	correlation_id int32
	id             string
	body           requestEncoder
}

func (r *request) Encode(pe enc.PacketEncoder) (err error) {
	pe.Push(&enc.LengthField{})
	pe.PutInt16(r.body.key())
	pe.PutInt16(r.body.version())
	pe.PutInt32(r.correlation_id)
	err = pe.PutString(r.id)
	if err != nil {
		return err
	}
	err = r.body.Encode(pe)
	if err != nil {
		return err
	}
	return pe.Pop()
}
