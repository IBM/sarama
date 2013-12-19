package sarama

import "fmt"

type responseHeader struct {
	length        int32
	correlationID int32
}

const maxMessageSize = 32 * 1024 * 1024 // 32MB

func (r *responseHeader) decode(pd packetDecoder) (err error) {
	r.length, err = pd.getInt32()
	if err != nil {
		return err
	}
	if r.length <= 4 || r.length > maxMessageSize {
		return DecodingError{Info: fmt.Sprintf("Message too large or too small. Got %d", r.length)}
	}

	r.correlationID, err = pd.getInt32()
	return err
}
