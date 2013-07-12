package kafka

type requestBody interface {
	encoder
	key() int16
	version() int16
	expectResponse() bool
	topics() []topicRequest
}

type request struct {
	correlation_id int32
	id             *string
	body           requestBody
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

func (r *request) expectResponse() bool {
	return r.body.expectResponse()
}
