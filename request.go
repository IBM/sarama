package kafka

type API struct {
	key     int16
	version int16
}

var (
	REQUEST_PRODUCE        = API{0, 0}
	REQUEST_FETCH          = API{1, 0}
	REQUEST_OFFSET         = API{2, 0}
	REQUEST_METADATA       = API{3, 0}
	REQUEST_LEADER_AND_ISR = API{4, 0}
	REQUEST_STOP_REPLICA   = API{5, 0}
	REQUEST_OFFSET_COMMIT  = API{6, 0}
	REQUEST_OFFSET_FETCH   = API{7, 0}
)

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
