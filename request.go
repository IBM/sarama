package kafka

type request struct {
	correlation_id int32
	id             *string
	body           apiEncoder
}

func (r *request) encode(pe packetEncoder) {
	pe.putInt16(r.body.key())
	pe.putInt16(r.body.version())
	pe.putInt32(r.correlation_id)
	pe.putString(r.id)
	r.body.encode(pe)
}
