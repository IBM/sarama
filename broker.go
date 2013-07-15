package kafka

import (
	"io"
	"net"
	"sync"
)

type Broker struct {
	id   int32
	host *string
	port int32

	correlation_id int32
	conn           net.Conn
	lock           sync.Mutex

	responses chan responsePromise
	done      chan bool
}

type responsePromise struct {
	correlation_id int32
	packets        chan []byte
	errors         chan error
}

func NewBroker(host string, port int32) *Broker {
	b := new(Broker)
	b.id = -1 // don't know it yet
	b.host = &host
	b.port = port
	return b
}

func (b *Broker) Connect() (err error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	addr, err := net.ResolveIPAddr("ip", *b.host)
	if err != nil {
		return err
	}

	b.conn, err = net.DialTCP("tcp", nil, &net.TCPAddr{IP: addr.IP, Port: int(b.port), Zone: addr.Zone})
	if err != nil {
		return err
	}

	b.done = make(chan bool)

	// permit a few outstanding requests before we block waiting for responses
	b.responses = make(chan responsePromise, 4)

	go b.responseReceiver()

	return nil
}

func (b *Broker) Close() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	close(b.responses)
	<-b.done

	err := b.conn.Close()

	b.conn = nil
	b.responses = nil

	return err
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

func (b *Broker) send(clientID *string, req requestEncoder, promiseResponse bool) (*responsePromise, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	fullRequest := request{b.correlation_id, clientID, req}
	buf, err := encode(&fullRequest)
	if err != nil {
		return nil, err
	}

	_, err = b.conn.Write(buf)
	if err != nil {
		return nil, err
	}
	b.correlation_id++

	if !promiseResponse {
		return nil, nil
	}

	promise := responsePromise{fullRequest.correlation_id, make(chan []byte), make(chan error)}
	b.responses <- promise

	return &promise, nil
}

func (b *Broker) sendAndReceive(clientID *string, req requestEncoder, res decoder) error {
	promise, err := b.send(clientID, req, res != nil)

	if err != nil {
		return err
	}

	if promise == nil {
		return nil
	}

	select {
	case buf := <-promise.packets:
		return decode(buf, res)
	case err = <-promise.errors:
		return err
	}
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

	return nil
}

func (b *Broker) responseReceiver() {
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
	close(b.done)
}
