package kafka

import (
	"encoding/binary"
	"errors"
	"math"
	"net"
)

type broker struct {
	nodeId int32
	host   *string
	port   int32

	correlation_id int32

	conn net.Conn
	addr net.TCPAddr

	requests  chan reqResPair
	responses chan reqResPair
}

type reqResPair struct {
	correlation_id int32
	packets        chan []byte
}

func newBroker(host string, port int32) (b *broker, err error) {
	b = new(broker)
	b.nodeId = -1 // don't know it yet
	b.host = &host
	b.port = port
	err = b.connect()
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (b *broker) connect() (err error) {
	addr, err := net.ResolveIPAddr("ip", *b.host)
	if err != nil {
		return err
	}

	b.addr.IP = addr.IP
	b.addr.Zone = addr.Zone
	b.addr.Port = int(b.port)

	b.conn, err = net.DialTCP("tcp", nil, &b.addr)
	if err != nil {
		return err
	}

	go b.sendRequestLoop()
	go b.rcvResponseLoop()

	return nil
}

func (b *broker) encode(pe packetEncoder) {
	pe.putInt32(b.nodeId)
	pe.putString(b.host)
	pe.putInt32(b.port)
}

func (b *broker) decode(pd packetDecoder) (err error) {
	b.nodeId, err = pd.getInt32()
	if err != nil {
		return err
	}

	b.host, err = pd.getString()
	if err != nil {
		return err
	}

	b.port, err = pd.getInt32()
	if err != nil {
		return err
	}

	err = b.connect()
	if err != nil {
		return err
	}

	return nil
}

func (b *broker) sendRequestLoop() {
	var request reqResPair
	var n int
	var err error
	var buf []byte
	for {
		request = <-b.requests
		buf = <-request.packets
		n, err = b.conn.Write(buf)
		if err != nil || n != len(buf) {
			close(b.requests)
			return
		}
		b.responses <- request
	}
}

func (b *broker) rcvResponseLoop() {
	var response reqResPair
	var n int
	var length int32
	var err error
	var buf []byte
	header := make([]byte, 4)
	for {
		response = <-b.responses
		n, err = b.conn.Read(header)
		if err != nil || n != 4 {
			close(b.responses)
			return
		}
		length = int32(binary.BigEndian.Uint32(header))
		if length <= 4 || length > 2*math.MaxUint16 {
			close(b.responses)
			return
		}

		n, err = b.conn.Read(header)
		if err != nil || n != 4 {
			close(b.responses)
			return
		}
		if response.correlation_id != int32(binary.BigEndian.Uint32(header)) {
			close(b.responses)
			return
		}

		buf = make([]byte, length-4)
		n, err = b.conn.Read(buf)
		if err != nil || n != int(length-4) {
			close(b.responses)
			return
		}

		response.packets <- buf
		close(response.packets)
	}
}

func (b *broker) sendRequest(clientID *string, api API, body encoder) (chan []byte, error) {
	var prepEnc prepEncoder
	var realEnc realEncoder

	req := request{api, b.correlation_id, clientID, body}

	req.encode(&prepEnc)
	if prepEnc.err {
		return nil, errors.New("kafka encoding error")
	}

	realEnc.raw = make([]byte, prepEnc.length+4)
	realEnc.putInt32(int32(prepEnc.length))
	req.encode(&realEnc)

	// we buffer one packet so that we can send our packet to the request queue without
	// blocking, and so that the responses can be sent to us async if we want them
	request := reqResPair{b.correlation_id, make(chan []byte, 1)}

	request.packets <- realEnc.raw
	b.requests <- request
	b.correlation_id++
	return request.packets, nil
}
