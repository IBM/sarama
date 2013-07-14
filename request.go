package kafka

// An internal interface satisfied by all of the Request structures
// (MetadataRequest, ProduceRequest, etc).
type RequestEncoder interface {
	encoder
	key() int16
	version() int16
	responseDecoder() decoder
}

type request struct {
	correlation_id int32
	id             *string
	body           RequestEncoder
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
