package sarama

import "time"

// many of our channels don't *need* any buffering at all, but go likely behaves more efficiently
// if the goroutine scheduler isn't forced to context-switch all the time by bufferless channels,
// so we define a bufferSize for that purpose
// TODO: benchmark to find the real optimum / verify this makes a difference
const efficientBufferSize = 32

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

// simple resettable timer and mock for when no timer is necessary
type timer interface {
	C() <-chan time.Time
	Reset()
}

type realTimer struct {
	t *time.Timer
	d time.Duration
}

func (t *realTimer) C() <-chan time.Time {
	return t.t.C
}

func (t *realTimer) Reset() {
	t.t.Reset(t.d)
}

type fakeTimer struct{}

func (t *fakeTimer) C() <-chan time.Time {
	return nil
}

func (t *fakeTimer) Reset() {}

// helper for launching goroutines with the appropriate panic handler
func withRecover(fn func()) {
	defer func() {
		if PanicHandler != nil {
			if err := recover(); err != nil {
				PanicHandler(err)
			}
		}
	}()

	fn()
}

// Encoder is a simple interface for any type that can be encoded as an array of bytes
// in order to be sent as the key or value of a Kafka message.
type Encoder interface {
	Encode() ([]byte, error)
}

// make strings and byte slices encodable for convenience so they can be used as keys
// and/or values in kafka messages

// StringEncoder implements the Encoder interface for Go strings so that you can do things like
//	producer.SendMessage(nil, sarama.StringEncoder("hello world"))
type StringEncoder string

func (s StringEncoder) Encode() ([]byte, error) {
	return []byte(s), nil
}

// ByteEncoder implements the Encoder interface for Go byte slices so that you can do things like
//	producer.SendMessage(nil, sarama.ByteEncoder([]byte{0x00}))
type ByteEncoder []byte

func (b ByteEncoder) Encode() ([]byte, error) {
	return b, nil
}
