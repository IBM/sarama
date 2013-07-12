package kafka

type requestAPI interface {
	key() int16
	version() int16
	expectResponse() bool
}

type requestEncoder interface {
	encoder
	requestAPI
}

type request struct {
	correlation_id int32
	id             *string
	body           requestEncoder
}

func (r *request) encode(pe packetEncoder) {
	pe.putInt16(r.body.key())
	pe.putInt16(r.body.version())
	pe.putInt32(r.correlation_id)
	pe.putString(r.id)
	r.body.encode(pe)
}
