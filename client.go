package kafka

import (
	"encoding/binary"
	"net"
)

type Client struct {
	addr string
	conn net.Conn
}

func NewClient(addr string) (client *Client, err error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	client = &Client{addr, conn}
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
