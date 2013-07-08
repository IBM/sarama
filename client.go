package kafka

import (
	"encoding/binary"
	"net"
)

type ApiKey int16
type ApiVersion int16

type API struct {
	key     ApiKey
	version ApiVersion
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
	responses      chan response
}

type response struct {
	correlation_id int32
	buf            []byte
}

func NewClient(addr string) (client *Client, err error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	client = &Client{addr, nil, 0, conn, make(chan response)}
	go client.readLoop()
	return client, err
}

func (client *Client) write(buf []byte) (err error) {
	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(len(buf)))
	_, err = client.conn.Write(size)
	if err != nil {
		return err
	}
	_, err = client.conn.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (client *Client) readLoop() {
	var resp response
	header := make([]byte, 4)
	for {
		n, err := client.conn.Read(header)
		if err != nil || n != 4 {
			close(client.responses)
			return
		}
		length := int32(binary.BigEndian.Uint32(header))
		if length <= 4 {
			close(client.responses)
			return
		}

		n, err = client.conn.Read(header)
		if err != nil || n != 4 {
			close(client.responses)
			return
		}
		resp.correlation_id = int32(binary.BigEndian.Uint32(header))

		resp.buf = make([]byte, length-4)
		n, err = client.conn.Read(resp.buf)
		if err != nil || n != int(length-4) {
			close(client.responses)
			return
		}

		client.responses <- resp
	}
}

func (client *Client) sendRequest(api API, body []byte) (err error) {
	idLen, err := stringLen(client.id)
	if err != nil {
		return err
	}
	buf := make([]byte, 4+idLen+len(body))
	off := 0
	binary.BigEndian.PutUint16(buf[off:], uint16(api.key))
	off += 2
	binary.BigEndian.PutUint16(buf[off:], uint16(api.version))
	off += 2
	binary.BigEndian.PutUint32(buf[off:], uint32(client.correlation_id))
	off += 4
	client.correlation_id++
	off, err = encodeString(buf, off, client.id)
	if err != nil {
		return err
	}
	copy(buf[off:], body)
	return client.write(buf)
}

func (client *Client) sendMetadataRequest(topics []string) (err error) {
	bufLen := 4
	for i := range topics {
		bufLen += len(topics[i])
	}
	buf := make([]byte, bufLen)
	off := 0
	binary.BigEndian.PutUint32(buf[off:], uint32(len(topics)))
	off += 4
	for i := range topics {
		off, err = encodeString(buf, off, &topics[i])
		if err != nil {
			return err
		}
	}
	return client.sendRequest(REQUEST_METADATA, buf)
}
