package sarama

import (
	"bufio"
	"net"
	"sort"
)

type none struct{}

// make []int32 sortable so we can sort partition numbers
type int32Slice []int32

func (slice int32Slice) Len() int {
	return len(slice)
}

func (slice int32Slice) Less(i, j int) bool {
	return slice[i] < slice[j]
}

func (slice int32Slice) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func dupeAndSort(input []int32) []int32 {
	ret := make([]int32, 0, len(input))
	for _, val := range input {
		ret = append(ret, val)
	}

	sort.Sort(int32Slice(ret))
	return ret
}

func withRecover(fn func()) {
	defer func() {
		handler := PanicHandler
		if handler != nil {
			if err := recover(); err != nil {
				handler(err)
			}
		}
	}()

	fn()
}

func safeAsyncClose(b *Broker) {
	tmp := b // local var prevents clobbering in goroutine
	go withRecover(func() {
		if connected, _ := tmp.Connected(); connected {
			if err := tmp.Close(); err != nil {
				Logger.Println("Error closing broker", tmp.ID(), ":", err)
			}
		}
	})
}

// Encoder is a simple interface for any type that can be encoded as an array of bytes
// in order to be sent as the key or value of a Kafka message. Length() is provided as an
// optimization, and must return the same as len() on the result of Encode().
type Encoder interface {
	Encode() ([]byte, error)
	Length() int
}

// make strings and byte slices encodable for convenience so they can be used as keys
// and/or values in kafka messages

// StringEncoder implements the Encoder interface for Go strings so that they can be used
// as the Key or Value in a ProducerMessage.
type StringEncoder string

func (s StringEncoder) Encode() ([]byte, error) {
	return []byte(s), nil
}

func (s StringEncoder) Length() int {
	return len(s)
}

// ByteEncoder implements the Encoder interface for Go byte slices so that they can be used
// as the Key or Value in a ProducerMessage.
type ByteEncoder []byte

func (b ByteEncoder) Encode() ([]byte, error) {
	return b, nil
}

func (b ByteEncoder) Length() int {
	return len(b)
}

// bufConn wraps a net.Conn with a buffer for reads to reduce the number of
// reads that trigger syscalls.
type bufConn struct {
	net.Conn
	buf *bufio.Reader
}

func newBufConn(conn net.Conn) *bufConn {
	return &bufConn{
		Conn: conn,
		buf:  bufio.NewReader(conn),
	}
}

func (bc *bufConn) Read(b []byte) (n int, err error) {
	return bc.buf.Read(b)
}

// An IntHeap is a min-heap of ints.
// https://golang.org/pkg/container/heap/#example__intHeap
type Int32Heap []int32

func (h Int32Heap) Len() int           { return len(h) }
func (h Int32Heap) Less(i, j int) bool { return h[i] < h[j] }
func (h Int32Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *Int32Heap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int32))
}

func (h *Int32Heap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func IntInSlice(ints []int32, int int32) bool {
	for _, i := range ints {
		if i == int {
			return true
		}
	}

	return false
}
