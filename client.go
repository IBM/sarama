package kafka

import (
	"encoding/binary"
	"math"
	"net"
	"strings"
)

type ApiKey uint16
type ApiVersion uint16

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
	r := strings.NewReader(in)
	size := r.Len()
	if size > math.MaxInt16 {
		panic("string too long to encode") /* Or just return nil? */
	}
	buf = make([]byte, 2+size)
	binary.BigEndian.PutUint16(buf, uint16(size))
	if size > 0 {
		_, err := r.Read(buf[2:])
		if err != nil {
			/* this should never happen */
			panic("couldn't read from string")
		}
	}
	return buf
}
