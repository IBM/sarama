package kafka

// Message is what is returned from fetch requests.
type Message struct {
	Offset int64
	Key    []byte
	Value  []byte
}

// (it doesn't quite line up with what kafka encodes as a message, but that's because
// theirs seems odd - it's missing the message offset for one thing...)
