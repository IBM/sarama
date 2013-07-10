package kafka

import (
	"encoding/binary"
	"io"
	"math"
	"net"
)

type broker struct {
	id   int32
	host *string
	port int32

	correlation_id int32

	conn net.Conn
	addr net.TCPAddr

	requests  chan responsePromise
	responses chan responsePromise
}

type responsePromise struct {
	correlation_id int32
	packets        chan []byte
	errors         chan error
}

func newBroker(host string, port int32) (b *broker, err error) {
	b = new(broker)
	b.id = -1 // don't know it yet
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
	go b.rcvresponsePromiseLoop()

	return nil
}

func (b *broker) forceDisconnect(reqRes *responsePromise, err error) {
	reqRes.errors <- err
	close(reqRes.errors)
	close(reqRes.packets)

	close(b.requests)
	close(b.responses)

	b.conn.Close()
}

func (b *broker) encode(pe packetEncoder) {
	pe.putInt32(b.id)
	pe.putString(b.host)
	pe.putInt32(b.port)
}

func (b *broker) decode(pd packetDecoder) (err error) {
	b.id, err = pd.getInt32()
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
	for request := range b.requests {
		buf := <-request.packets
		_, err := b.conn.Write(buf)
		if err != nil {
			b.forceDisconnect(&request, err)
			return
		}
		b.responses <- request
	}
}

func (b *broker) rcvresponsePromiseLoop() {
	header := make([]byte, 4)
	for response := range b.responses {
		_, err := io.ReadFull(b.conn, header)
		if err != nil {
			b.forceDisconnect(&response, err)
			return
		}

		length := int32(binary.BigEndian.Uint32(header))
		if length <= 4 || length > 2*math.MaxUint16 {
			b.forceDisconnect(&response, DecodingError{})
			return
		}

		_, err = io.ReadFull(b.conn, header)
		if err != nil {
			b.forceDisconnect(&response, err)
			return
		}
		if response.correlation_id != int32(binary.BigEndian.Uint32(header)) {
			b.forceDisconnect(&response, DecodingError{})
			return
		}

		buf := make([]byte, length-4)
		_, err = io.ReadFull(b.conn, buf)
		if err != nil {
			b.forceDisconnect(&response, err)
			return
		}

		response.packets <- buf
		close(response.packets)
		close(response.errors)
	}
}

func (b *broker) sendRequest(clientID *string, body encoder) (*responsePromise, error) {
	var prepEnc prepEncoder
	var realEnc realEncoder
	var api API

	switch body.(type) {
	case *metadataRequest:
		api = REQUEST_METADATA
	default:
		return nil, EncodingError{}
	}

	req := request{api, b.correlation_id, clientID, body}

	req.encode(&prepEnc)
	if prepEnc.err {
		return nil, EncodingError{}
	}

	realEnc.raw = make([]byte, prepEnc.length+4)
	realEnc.putInt32(int32(prepEnc.length))
	req.encode(&realEnc)

	// we buffer one packet and one error so that all this can work async if the
	// caller so desires. we also cheat and use the same responsePromise object for both the
	// request and the response, as things are much simpler that way
	request := responsePromise{b.correlation_id, make(chan []byte, 1), make(chan error, 1)}

	request.packets <- realEnc.raw
	b.requests <- request
	b.correlation_id++
	return &request, nil
}
