package sarama

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

// BrokerConfig is used to pass multiple configuration options to Broker.Open.
type BrokerConfig struct {
	MaxOpenRequests int // How many outstanding requests the broker is allowed to have before blocking attempts to send (default 5).

	// All three of the below configurations are similar to the `socket.timeout.ms` setting in JVM kafka.
	DialTimeout  time.Duration // How long to wait for the initial connection to succeed before timing out and returning an error (default 30s).
	ReadTimeout  time.Duration // How long to wait for a response before timing out and returning an error (default 30s).
	WriteTimeout time.Duration // How long to wait for a transmit to succeed before timing out and returning an error (default 30s).
}

// NewBrokerConfig returns a new broker configuration with sane defaults.
func NewBrokerConfig() *BrokerConfig {
	return &BrokerConfig{
		MaxOpenRequests: 5,
		DialTimeout:     30 * time.Second,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
	}
}

// Validate checks a BrokerConfig instance. This will return a
// ConfigurationError if the specified values don't make sense.
func (config *BrokerConfig) Validate() error {
	if config.MaxOpenRequests <= 0 {
		return ConfigurationError("Invalid MaxOpenRequests")
	}

	if config.DialTimeout <= 0 {
		return ConfigurationError("Invalid DialTimeout")
	}

	if config.ReadTimeout <= 0 {
		return ConfigurationError("Invalid ReadTimeout")
	}

	if config.WriteTimeout <= 0 {
		return ConfigurationError("Invalid WriteTimeout")
	}

	return nil
}

// Broker represents a single Kafka broker connection. All operations on this object are entirely concurrency-safe.
type Broker struct {
	id   int32
	addr string

	conf          *BrokerConfig
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
// The only errors Open will return directly are ConfigurationError or AlreadyConnected. If conf is nil, the result of
// NewBrokerConfig() is used.
func (b *Broker) Open(conf *BrokerConfig) error {
	if conf == nil {
		conf = NewBrokerConfig()
	}

	err := conf.Validate()
	if err != nil {
		return err
	}

	b.lock.Lock()

	if b.conn != nil {
		b.lock.Unlock()
		Logger.Printf("Failed to connect to broker %s: %s\n", b.addr, AlreadyConnected)
		return AlreadyConnected
	}

	go withRecover(func() {
		defer b.lock.Unlock()

		b.conn, b.connErr = net.DialTimeout("tcp", b.addr, conf.DialTimeout)
		if b.connErr != nil {
			b.conn = nil
			Logger.Printf("Failed to connect to broker %s: %s\n", b.addr, b.connErr)
			return
		}

		b.conf = conf
		b.done = make(chan bool)
		b.responses = make(chan responsePromise, b.conf.MaxOpenRequests-1)

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
			Logger.Printf("Failed to close connection to broker %s: %s\n", b.addr, err)
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

func (b *Broker) GetConsumerMetadata(clientID string, request *ConsumerMetadataRequest) (*ConsumerMetadataResponse, error) {
	response := new(ConsumerMetadataResponse)

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

	err = b.conn.SetWriteDeadline(time.Now().Add(b.conf.WriteTimeout))
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
		err := b.conn.SetReadDeadline(time.Now().Add(b.conf.ReadTimeout))
		if err != nil {
			response.errors <- err
			continue
		}

		_, err = io.ReadFull(b.conn, header)
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
			// TODO if decoded ID < cur ID, discard until we catch up
			// TODO if decoded ID > cur ID, save it so when cur ID catches up we have a response
			response.errors <- DecodingError{
				Info: fmt.Sprintf("CorrelationID didn't match, wanted %d, got %d", response.correlationID, decodedHeader.correlationID),
			}
			continue
		}

		buf := make([]byte, decodedHeader.length-4)
		_, err = io.ReadFull(b.conn, buf)
		if err != nil {
			// XXX: the above ReadFull call inherits the same ReadDeadline set at the top of this loop, so it may
			// fail with a timeout error. If this happens, our connection is permanently toast since we will no longer
			// be aligned correctly on the stream (we'll be reading garbage Kafka headers from the middle of data).
			// Can we/should we fail harder in that case?
			response.errors <- err
			continue
		}

		response.packets <- buf
	}
	close(b.done)
}
