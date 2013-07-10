package kafka

type request struct {
	api            API
	correlation_id int32
	id             *string
	body           encoder
}

func (r *request) encode(pe packetEncoder) {
	pe.putInt16(r.api.key)
	pe.putInt16(r.api.version)
	pe.putInt32(r.correlation_id)
	pe.putString(r.id)
	r.body.encode(pe)
}
