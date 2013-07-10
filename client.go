package kafka

import (
	"encoding/binary"
	"errors"
	"math"
	"net"
)

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

type Client struct {
	addr           string
	id             *string
	correlation_id int32
	conn           net.Conn
	requests       chan reqResPair
	responses      chan reqResPair
}

type reqResPair struct {
	correlation_id int32
	packets        chan []byte
}

func NewClient(addr string) (client *Client, err error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	client = &Client{addr: addr, conn: conn, requests: make(chan reqResPair), responses: make(chan reqResPair)}
	go client.sendRequestLoop()
	go client.rcvResponseLoop()
	return client, err
}

func (client *Client) sendRequestLoop() {
	var request reqResPair
	var n int
	var err error
	var buf []byte
	for {
		request = <-client.requests
		buf = <-request.packets
		n, err = client.conn.Write(buf)
		if err != nil || n != len(buf) {
			close(client.requests)
			return
		}
		client.responses <- request
	}
}

func (client *Client) rcvResponseLoop() {
	var response reqResPair
	var n int
	var length int32
	var err error
	var buf []byte
	header := make([]byte, 4)
	for {
		response = <-client.responses
		n, err = client.conn.Read(header)
		if err != nil || n != 4 {
			close(client.responses)
			return
		}
		length = int32(binary.BigEndian.Uint32(header))
		if length <= 4 || length > 2*math.MaxUint16 {
			close(client.responses)
			return
		}

		n, err = client.conn.Read(header)
		if err != nil || n != 4 {
			close(client.responses)
			return
		}
		if response.correlation_id != int32(binary.BigEndian.Uint32(header)) {
			close(client.responses)
			return
		}

		buf = make([]byte, length-4)
		n, err = client.conn.Read(buf)
		if err != nil || n != int(length-4) {
			close(client.responses)
			return
		}

		response.packets <- buf
		close(response.packets)
	}
}

func (client *Client) encode(api API, body []byte, pe packetEncoder) {
	pe.putInt32(int32(len(body)))
	pe.putInt16(api.key)
	pe.putInt16(api.version)
	pe.putInt32(client.correlation_id)
	pe.putString(client.id)
	//pe.putRaw(body)
}

func (client *Client) sendRequest(api API, body encoder) (chan []byte, error) {
	var prepEnc prepEncoder
	var realEnc realEncoder

	req := request{api, client.correlation_id, client.id, body}

	req.encode(&prepEnc)
	if prepEnc.err {
		return nil, errors.New("kafka encoding error")
	}

	realEnc.raw = make([]byte, prepEnc.length+4)
	realEnc.putInt32(int32(prepEnc.length))
	req.encode(&realEnc)

	// we buffer one packet so that we can send our packet to the request queue without
	// blocking, and so that the responses can be sent to us async if we want them
	request := reqResPair{client.correlation_id, make(chan []byte, 1)}

	request.packets <- realEnc.raw
	client.requests <- request
	client.correlation_id++
	return request.packets, nil
}
