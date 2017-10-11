package sarama

import "encoding/binary"

const (
	controlMask = 0x20
)

type Header struct {
	Key   []byte
	Value []byte
}

func (h *Header) encode(pe packetEncoder) error {
	if err := pe.putVarintBytes(h.Key); err != nil {
		return err
	}
	return pe.putVarintBytes(h.Value)
}

func (h *Header) decode(pd packetDecoder) (err error) {
	if h.Key, err = pd.getVarintBytes(); err != nil {
		return err
	}

	if h.Value, err = pd.getVarintBytes(); err != nil {
		return err
	}
	return nil
}

type Record struct {
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int64
	Key            []byte
	Value          []byte
	Headers        []*Header

	lengthComputed bool
	length         int64
	totalLength    int
}

func (r *Record) encode(pe packetEncoder) error {
	if err := r.computeLength(); err != nil {
		return err
	}

	pe.putVarint(r.length)
	pe.putInt8(r.Attributes)
	pe.putVarint(r.TimestampDelta)
	pe.putVarint(r.OffsetDelta)
	pe.putVarintBytes(r.Key)
	pe.putVarintBytes(r.Value)
	pe.putVarint(int64(len(r.Headers)))

	for _, h := range r.Headers {
		if err := h.encode(pe); err != nil {
			return err
		}
	}

	return nil
}

func (r *Record) decode(pd packetDecoder) (err error) {
	length, err := newVarintLengthField(pd)
	if err != nil {
		return err
	}
	if err = pd.push(length); err != nil {
		return err
	}
	r.length = length.length
	r.lengthComputed = true

	if r.Attributes, err = pd.getInt8(); err != nil {
		return err
	}

	if r.TimestampDelta, err = pd.getVarint(); err != nil {
		return err
	}

	if r.OffsetDelta, err = pd.getVarint(); err != nil {
		return err
	}

	if r.Key, err = pd.getVarintBytes(); err != nil {
		return err
	}

	if r.Value, err = pd.getVarintBytes(); err != nil {
		return err
	}

	numHeaders, err := pd.getVarint()
	if err != nil {
		return err
	}

	if numHeaders >= 0 {
		r.Headers = make([]*Header, numHeaders)
	}
	for i := int64(0); i < numHeaders; i++ {
		hdr := new(Header)
		hdr.decode(pd)
		r.Headers[i] = hdr
	}

	return pd.pop()
}

// Because the length is varint we can't reserve a fixed amount of bytes for it.
// We use the prepEncoder to figure out the length of the record and then we cache it.
func (r *Record) computeLength() error {
	if !r.lengthComputed {
		r.lengthComputed = true

		var prep prepEncoder
		if err := r.encode(&prep); err != nil {
			return err
		}
		// subtract 1 because we don't want to include the length field itself (which 1 byte, the
		// length of varint encoding of 0)
		r.length = int64(prep.length) - 1
	}

	return nil
}

func (r *Record) getTotalLength() (int, error) {
	if r.totalLength == 0 {
		if err := r.computeLength(); err != nil {
			return 0, err
		}
		var buf [binary.MaxVarintLen64]byte
		r.totalLength = int(r.length) + binary.PutVarint(buf[:], r.length)
	}

	return r.totalLength, nil
}
