package kafka

type requestEncoder interface {
	encoder
	key() int16
	version() int16
}

type request struct {
	correlation_id int32
	id             string
	body           requestEncoder
}

func (r *request) encode(pe packetEncoder) (err error) {
	pe.push(&lengthField{})
	pe.putInt16(r.body.key())
	pe.putInt16(r.body.version())
	pe.putInt32(r.correlation_id)
	err = pe.putString(r.id)
	if err != nil {
		return err
	}
	err = r.body.encode(pe)
	if err != nil {
		return err
	}
	return pe.pop()
}
