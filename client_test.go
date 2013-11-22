package sarama

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestSimpleClient(t *testing.T) {
	responses := make(chan []byte, 1)
	mockBroker := NewMockBroker(t, responses)
	defer mockBroker.Close()

	// Only one response needed, an empty metadata response
	responses <- []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

	client, err := NewClient("client_id", []string{mockBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
}

func TestClientExtraBrokers(t *testing.T) {
	responses := make(chan []byte, 1)
	mockBroker := NewMockBroker(t, responses)
	mockExtra := NewMockBroker(t, make(chan []byte))
	defer mockBroker.Close()
	defer mockExtra.Close()

	// return the extra mock as another available broker
	response := []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}
	binary.BigEndian.PutUint32(response[19:], uint32(mockExtra.Port()))
	responses <- response

	client, err := NewClient("client_id", []string{mockBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
}

func TestClientMetadata(t *testing.T) {
	responses := make(chan []byte, 1)
	mockBroker := NewMockBroker(t, responses)
	mockExtra := NewMockBroker(t, make(chan []byte))
	defer mockBroker.Close()
	defer mockExtra.Close()

	// return the extra mock as another available broker
	response := []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x05,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x05,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}
	binary.BigEndian.PutUint32(response[19:], uint32(mockExtra.Port()))
	responses <- response

	client, err := NewClient("client_id", []string{mockBroker.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topics, err := client.Topics()
	if err != nil {
		t.Error(err)
	} else if len(topics) != 1 || topics[0] != "my_topic" {
		t.Error("Client returned incorrect topics:", topics)
	}

	parts, err := client.Partitions("my_topic")
	if err != nil {
		t.Error(err)
	} else if len(parts) != 1 || parts[0] != 0 {
		t.Error("Client returned incorrect partitions for my_topic:", parts)
	}

	tst, err := client.Leader("my_topic", 0)
	if err != nil {
		t.Error(err)
	} else if tst.ID() != 5 {
		t.Error("Leader for my_topic had incorrect ID.")
	}
}

func TestClientRefreshBehaviour(t *testing.T) {
	responses := make(chan []byte, 1)
	extraResponses := make(chan []byte, 2)
	mockBroker := NewMockBroker(t, responses)
	mockExtra := NewMockBroker(t, extraResponses)
	defer mockBroker.Close()
	defer mockExtra.Close()

	// return the extra mock as another available broker
	response := []byte{
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0xaa,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}
	binary.BigEndian.PutUint32(response[19:], uint32(mockExtra.Port()))
	responses <- response
	extraResponses <- []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x05,
		0x00, 0x00, 0x00, 0x0e,
		0xFF, 0xFF, 0xFF, 0xFF,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}
	extraResponses <- []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c',
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00,
		0x00, 0x00, 0x00, 0x0b,
		0x00, 0x00, 0x00, 0xaa,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}

	client, err := NewClient("clientID", []string{mockBroker.Addr()}, &ClientConfig{MetadataRetries: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	parts, err := client.Partitions("my_topic")
	if err != nil {
		t.Error(err)
	} else if len(parts) != 1 || parts[0] != 0xb {
		t.Error("Client returned incorrect partitions for my_topic:", parts)
	}

	tst, err := client.Leader("my_topic", 0xb)
	if err != nil {
		t.Error(err)
	} else if tst.ID() != 0xaa {
		t.Error("Leader for my_topic had incorrect ID.")
	}

	client.disconnectBroker(tst)
}

func TestClientTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error("Failed to create a listener, err:%s", err)
		return
	}
	defer listener.Close()

	connChannel := make(chan net.Conn, 1)
	socketErrorChannel := make(chan error, 1)
	clientChannel := make(chan *Client, 1)
	clientErrorChannel := make(chan error, 1)

	// spin up a simple server that will accept connections but send/receive no data
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				socketErrorChannel <- err
				continue
			}
			connChannel <- conn
		}
	}()

	go func() {
		addr := listener.Addr()
		config := &ClientConfig{
			MetadataRetries:      1,
			WaitForElection:      1 * time.Second,
			ConcurrencyPerBroker: 1,
			ConnectTimeout:       1 * time.Second}
		client, err := NewClient("TestClientTimeout", []string{addr.String()}, config)
		if err != nil {
			clientErrorChannel <- err
			return
		}
		clientChannel <- client
	}()

	connected := false
	for {
		select {
		case <-connChannel:
			connected = true
		case err := <-socketErrorChannel:
			t.Error("Unexpected socket error:%v", err)
			return
		case client := <-clientChannel:
			t.Error("Connect should have failed:%v", client)
			return
		case err := <-clientErrorChannel:
			if !connected {
				t.Error("Unexpected error during NewClient(), error:%v", err)
			}
			if err != OutOfBrokers {
				t.Error("Unexpected error during NewClient(), error:%v", err)
			}
			return
		case <-time.After(time.Second * 5):
			t.Error("Failed to detect a socket timeout")
			return
		}
	}
}
