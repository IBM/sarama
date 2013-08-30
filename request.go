package sarama

type requestEncoder interface {
	encoder
	key() int16
	version() int16
}

type request struct {
	correlationID int32
	id            string
	body          requestEncoder
}

func (r *request) encode(pe packetEncoder) (err error) {
	pe.push(&lengthField{})
	pe.putInt16(r.body.key())
	pe.putInt16(r.body.version())
	pe.putInt32(r.correlationID)
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
