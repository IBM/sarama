package sarama

import "fmt"

type requestBody interface {
	encoder
	decoder
	key() int16
	version() int16
}

type request struct {
	correlationID int32
	clientID      string
	body          requestBody
}

func (r *request) encode(pe packetEncoder) (err error) {
	pe.push(&lengthField{})
	pe.putInt16(r.body.key())
	pe.putInt16(r.body.version())
	pe.putInt32(r.correlationID)
	err = pe.putString(r.clientID)
	if err != nil {
		return err
	}
	err = r.body.encode(pe)
	if err != nil {
		return err
	}
	return pe.pop()
}

func (r *request) decode(pd packetDecoder) (err error) {
	var key int16
	if key, err = pd.getInt16(); err != nil {
		return err
	}
	var version int16
	if version, err = pd.getInt16(); err != nil {
		return err
	}
	if r.correlationID, err = pd.getInt32(); err != nil {
		return err
	}
	r.clientID, err = pd.getString()

	r.body = allocateBody(key, version)
	if r.body == nil {
		return PacketDecodingError{fmt.Sprintf("unknown request key (%d)", key)}
	}
	return r.body.decode(pd)
}

func allocateBody(key, version int16) requestBody {
	switch key {
	case 0:
		return &ProduceRequest{}
	case 1:
		return &FetchRequest{}
	case 2:
		return &OffsetRequest{}
	case 3:
		return &MetadataRequest{}
	case 8:
		return &OffsetCommitRequest{Version: version}
	case 9:
		return &OffsetFetchRequest{}
	case 10:
		return &ConsumerMetadataRequest{}
	}
	return nil
}
