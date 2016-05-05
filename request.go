package sarama

import (
	"encoding/binary"
	"fmt"
	"io"
)

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

func decodeRequest(r io.Reader) (req *request, err error) {
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBytes); err != nil {
		return nil, err
	}

	length := int32(binary.BigEndian.Uint32(lengthBytes))
	if length <= 4 || length > MaxRequestSize {
		return nil, PacketDecodingError{fmt.Sprintf("message of length %d too large or too small", length)}
	}

	encodedReq := make([]byte, length)
	if _, err := io.ReadFull(r, encodedReq); err != nil {
		return nil, err
	}

	req = &request{}
	if err := decode(encodedReq, req); err != nil {
		return nil, err
	}
	return req, nil
}

func allocateBody(key int16, version int16) requestBody {
	switch key {
	case 0:
		switch version {
		case 2:
			return &ProduceRequest{KafkaVersion: &KafkaVersion{Release: V0_10_0}}
		case 1:
			return &ProduceRequest{KafkaVersion: &KafkaVersion{Release: V0_9_0_0}}
		case 0:
			return &ProduceRequest{KafkaVersion: &KafkaVersion{Release: V0_8_2_2}}
		}
	case 1:
		switch version {
		case 2:
			return &FetchRequest{KafkaVersion: &KafkaVersion{Release: V0_10_0}}
		case 1:
			return &FetchRequest{KafkaVersion: &KafkaVersion{Release: V0_9_0_0}}
		case 0:
			return &FetchRequest{KafkaVersion: &KafkaVersion{Release: V0_8_2_2}}
		}
	case 2:
		return &OffsetRequest{}
	case 3:
		return &MetadataRequest{}
	case 8:
		switch version {
		case 2:
			return &OffsetCommitRequest{KafkaVersion: &KafkaVersion{Release: V0_9_0_0}}
		case 1:
			return &OffsetCommitRequest{KafkaVersion: &KafkaVersion{Release: V0_8_2_0}}
		case 0:
			return &OffsetCommitRequest{KafkaVersion: &KafkaVersion{Release: V0_8_1_0}}
		}
	case 9:
		switch version {
		case 1:
			return &OffsetFetchRequest{KafkaVersion: &KafkaVersion{Release: V0_8_2_0}}
		case 0:
			return &OffsetFetchRequest{KafkaVersion: &KafkaVersion{Release: V0_8_1_0}}
		}
	case 10:
		return &ConsumerMetadataRequest{}
	case 11:
		return &JoinGroupRequest{}
	case 12:
		return &HeartbeatRequest{}
	case 13:
		return &LeaveGroupRequest{}
	case 14:
		return &SyncGroupRequest{}
	case 15:
		return &DescribeGroupsRequest{}
	case 16:
		return &ListGroupsRequest{}
	}
	return nil
}
