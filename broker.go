package kafka

import (
	"io"
	"math"
	"net"
)

type Broker struct {
	id   int32
	host *string
	port int32

	correlation_id int32

	conn net.Conn

	requests  chan requestToSend
	responses chan responsePromise
}

type responsePromise struct {
	correlation_id int32
	packets        chan []byte
	errors         chan error
}

type requestToSend struct {
	// we cheat and use the responsePromise channels to avoid creating more than necessary
	response       responsePromise
	expectResponse bool
}

func NewBroker(host string, port int32) (b *Broker, err error) {
	b = new(Broker)
	b.id = -1 // don't know it yet
	b.host = &host
	b.port = port
	err = b.Connect()
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Broker) Connect() (err error) {
	addr, err := net.ResolveIPAddr("ip", *b.host)
	if err != nil {
		return err
	}

	b.conn, err = net.DialTCP("tcp", nil, &net.TCPAddr{IP: addr.IP, Port: int(b.port), Zone: addr.Zone})
	if err != nil {
		return err
	}

	b.requests = make(chan requestToSend)
	b.responses = make(chan responsePromise)

	go b.sendRequestLoop()
	go b.rcvResponseLoop()

	return nil
}

func (b *Broker) Close() error {
	close(b.requests)
	close(b.responses)

	return b.conn.Close()
}

func (b *Broker) RequestMetadata(clientID *string, request *MetadataRequest) (*MetadataResponse, error) {
	response := new(MetadataResponse)

	err := b.sendAndReceive(clientID, request, response)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) Produce(clientID *string, request *ProduceRequest) (*ProduceResponse, error) {
	var response *ProduceResponse
	if request.ResponseCondition != NO_RESPONSE {
		response = new(ProduceResponse)
	}

	err := b.sendAndReceive(clientID, request, response)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) sendAndReceive(clientID *string, req requestEncoder, res decoder) error {
	fullRequest := request{b.correlation_id, clientID, req}
	packet, err := encode(&fullRequest)
	if err != nil {
		return err
	}

	sendRequest := requestToSend{responsePromise{b.correlation_id, make(chan []byte), make(chan error)}, res != nil}

	b.requests <- sendRequest
	sendRequest.response.packets <- packet // we cheat to avoid poofing up more channels than necessary
	b.correlation_id++

	select {
	case buf := <-sendRequest.response.packets:
		err = decode(buf, res)
	case err = <-sendRequest.response.errors:
	}

	if err != nil {
		return err
	}

	return nil
}

func (b *Broker) encode(pe packetEncoder) {
	pe.putInt32(b.id)
	pe.putString(b.host)
	pe.putInt32(b.port)
}

func (b *Broker) decode(pd packetDecoder) (err error) {
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
	if b.port > math.MaxUint16 {
		return DecodingError("Broker port > 65536")
	}

	return nil
}

func (b *Broker) sendRequestLoop() {
	for request := range b.requests {
		buf := <-request.response.packets
		_, err := b.conn.Write(buf)
		if err != nil {
			request.response.errors <- err
			continue
		}
		if request.expectResponse {
			b.responses <- request.response
		}
	}
}

func (b *Broker) rcvResponseLoop() {
	header := make([]byte, 8)
	for response := range b.responses {
		_, err := io.ReadFull(b.conn, header)
		if err != nil {
			response.errors <- err
			continue
		}

		decodedHeader := responseHeader{}
		err = decode(header, &decodedHeader)
		if err != nil {
			response.errors <- err
			continue
		}
		if decodedHeader.correlation_id != response.correlation_id {
			response.errors <- DecodingError("Mismatched correlation id.")
			continue
		}

		buf := make([]byte, decodedHeader.length-4)
		_, err = io.ReadFull(b.conn, buf)
		if err != nil {
			response.errors <- err
			continue
		}

		response.packets <- buf
	}
}
