package sarama

import "fmt"

type responseHeader struct {
	length        int32
	correlationID int32
}

func (r *responseHeader) decode(pd packetDecoder) (err error) {
	r.length, err = pd.getInt32()
	if err != nil {
		return err
	}
	if r.length <= 4 || r.length > MaxResponseSize {
		return DecodingError{Info: fmt.Sprintf("Message too large or too small. Got %d", r.length)}
	}

	r.correlationID, err = pd.getInt32()
	return err
}
