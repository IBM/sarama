package sarama

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
)

// Broker represents a single Kafka broker connection. All operations on this object are entirely concurrency-safe.
type Broker struct {
	id   int32
	addr string

	correlationID int32
	conn          net.Conn
	connErr       error
	lock          sync.Mutex

	responses chan responsePromise
	done      chan bool
}

type responsePromise struct {
	correlationID int32
	packets       chan []byte
	errors        chan error
}

// NewBroker creates and returns a Broker targetting the given host:port address.
// This does not attempt to actually connect, you have to call Open() for that.
func NewBroker(addr string) *Broker {
	return &Broker{id: -1, addr: addr}
}

// Open tries to connect to the Broker. It takes the broker lock synchronously, then spawns a goroutine which
// connects and releases the lock. This means any subsequent operations on the broker will block waiting for
// the connection to finish. To get the effect of a fully synchronous Open call, follow it by a call to Connected().
// The only error Open will return directly is AlreadyConnected. The maxOpenRequests parameter determines how many
// requests can be issued concurrently before future requests block. You generally will want at least one for each
// topic-partition the broker will be interacting with concurrently.
func (b *Broker) Open(maxOpenRequests int) error {
	b.lock.Lock()

	if b.conn != nil {
		b.lock.Unlock()
		Logger.Printf("Failed to connect to broker %s\n", b.addr)
		Logger.Println(AlreadyConnected)
		return AlreadyConnected
	}

	go withRecover(func() {
		defer b.lock.Unlock()

		b.conn, b.connErr = net.Dial("tcp", b.addr)
		if b.connErr != nil {
			Logger.Printf("Failed to connect to broker %s\n", b.addr)
			Logger.Println(b.connErr)
			return
		}

		b.done = make(chan bool)

		// permit a few outstanding requests before we block waiting for responses
		b.responses = make(chan responsePromise, maxOpenRequests)

		Logger.Printf("Connected to broker %s\n", b.addr)
		go withRecover(b.responseReceiver)
	})

	return nil
}

// Connected returns true if the broker is connected and false otherwise. If the broker is not
// connected but it had tried to connect, the error from that connection attempt is also returned.
func (b *Broker) Connected() (bool, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.conn != nil, b.connErr
}

func (b *Broker) Close() (err error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	defer func() {
		if err == nil {
			Logger.Printf("Closed connection to broker %s\n", b.addr)
		} else {
			Logger.Printf("Failed to close connection to broker %s.\n", b.addr)
			Logger.Println(err)
		}
	}()

	if b.conn == nil {
		return NotConnected
	}

	close(b.responses)
	<-b.done

	err = b.conn.Close()

	b.conn = nil
	b.connErr = nil
	b.done = nil
	b.responses = nil

	return
}

// ID returns the broker ID retrieved from Kafka's metadata, or -1 if that is not known.
func (b *Broker) ID() int32 {
	return b.id
}

// Addr returns the broker address as either retrieved from Kafka's metadata or passed to NewBroker.
func (b *Broker) Addr() string {
	return b.addr
}

func (b *Broker) GetMetadata(clientID string, request *MetadataRequest) (*MetadataResponse, error) {
	response := new(MetadataResponse)

	err := b.sendAndReceive(clientID, request, response)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) GetAvailableOffsets(clientID string, request *OffsetRequest) (*OffsetResponse, error) {
	response := new(OffsetResponse)

	err := b.sendAndReceive(clientID, request, response)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) Produce(clientID string, request *ProduceRequest) (*ProduceResponse, error) {
	var response *ProduceResponse
	var err error

	if request.RequiredAcks == NoResponse {
		err = b.sendAndReceive(clientID, request, nil)
	} else {
		response = new(ProduceResponse)
		err = b.sendAndReceive(clientID, request, response)
	}

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) Fetch(clientID string, request *FetchRequest) (*FetchResponse, error) {
	response := new(FetchResponse)

	err := b.sendAndReceive(clientID, request, response)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) CommitOffset(clientID string, request *OffsetCommitRequest) (*OffsetCommitResponse, error) {
	response := new(OffsetCommitResponse)

	err := b.sendAndReceive(clientID, request, response)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) FetchOffset(clientID string, request *OffsetFetchRequest) (*OffsetFetchResponse, error) {
	response := new(OffsetFetchResponse)

	err := b.sendAndReceive(clientID, request, response)

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) send(clientID string, req requestEncoder, promiseResponse bool) (*responsePromise, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.conn == nil {
		if b.connErr != nil {
			return nil, b.connErr
		}
		return nil, NotConnected
	}

	fullRequest := request{b.correlationID, clientID, req}
	buf, err := encode(&fullRequest)
	if err != nil {
		return nil, err
	}

	_, err = b.conn.Write(buf)
	if err != nil {
		return nil, err
	}
	b.correlationID++

	if !promiseResponse {
		return nil, nil
	}

	promise := responsePromise{fullRequest.correlationID, make(chan []byte), make(chan error)}
	b.responses <- promise

	return &promise, nil
}

func (b *Broker) sendAndReceive(clientID string, req requestEncoder, res decoder) error {
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

	// For backward compatibility with go1.0
	return nil
}

func (b *Broker) decode(pd packetDecoder) (err error) {
	b.id, err = pd.getInt32()
	if err != nil {
		return err
	}

	host, err := pd.getString()
	if err != nil {
		return err
	}

	port, err := pd.getInt32()
	if err != nil {
		return err
	}

	b.addr = fmt.Sprint(host, ":", port)

	return nil
}

func (b *Broker) encode(pe packetEncoder) (err error) {

	host, portstr, err := net.SplitHostPort(b.addr)
	if err != nil {
		return err
	}
	port, err := strconv.Atoi(portstr)
	if err != nil {
		return err
	}

	pe.putInt32(b.id)

	err = pe.putString(host)
	if err != nil {
		return err
	}

	pe.putInt32(int32(port))

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
		if decodedHeader.correlationID != response.correlationID {
			response.errors <- DecodingError{Info: "CorrelationID didn't match"}
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
