package kafka

import (
	"encoding/binary"
	"math"
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
	addr, id       string
	correlation_id int32
	conn           net.Conn
}

func NewClient(addr string) (client *Client, err error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	client = &Client{addr, "", 0, conn}
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

func (client *Client) read() (buf []byte, err error) {
	size := make([]byte, 4)
	n, err := client.conn.Read(size)
	if err != nil {
		return nil, err
	}
	if n != 4 {
		return nil, nil
	}
	s := binary.BigEndian.Uint32(size)
	buf = make([]byte, s)
	n, err = client.conn.Read(buf)
	if err != nil {
		return nil, err
	}
	if uint32(n) != s {
		return nil, nil
	}
	return buf, nil
}

func encodeString(in string) (buf []byte) {
	size := len(in)
	if size > math.MaxInt16 {
		panic("string too long to encode") /* Or just return nil? */
	}
	buf = make([]byte, 2+size)
	binary.BigEndian.PutUint16(buf, uint16(size))
	if size > 0 {
		copy(buf[2:], in)
	}
	return buf
}

func encodeBytes(in []byte) (buf []byte) {
	size := len(in)
	if size > math.MaxInt32 {
		panic("bytes too long to encode") /* Or just return nil? */
	}
	buf = make([]byte, 4+size)
	binary.BigEndian.PutUint32(buf, uint32(size))
	if size > 0 {
		copy(buf[4:], in)
	}
	return buf
}

func (client *Client) sendRequest(api *API, body []byte) (err error) {
	id := encodeString(client.id)
	buf := make([]byte, 4+len(id)+len(body))
	binary.BigEndian.PutUint16(buf[0:2], uint16(api.key))
	binary.BigEndian.PutUint16(buf[2:4], uint16(api.version))
	binary.BigEndian.PutUint32(buf[4:8], uint32(client.correlation_id))
	copy(buf[8:], id)
	copy(buf[8+len(id):], body)
	return client.write(buf)
}
