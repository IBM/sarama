package kafka

type requestEncoder interface {
	encoder
	key() int16
	version() int16
}

type request struct {
	correlation_id int32
	id             *string
	body           requestEncoder
}

func (r *request) encode(pe packetEncoder) {
	pe.pushLength32()
	pe.putInt16(r.body.key())
	pe.putInt16(r.body.version())
	pe.putInt32(r.correlation_id)
	pe.putString(r.id)
	r.body.encode(pe)
	pe.pop()
}
