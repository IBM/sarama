/*
Package sarama provides client libraries for the Kafka 0.8 protocol. The Client, Producer and Consumer objects are the core of the high-level API. The Broker and Request/Response objects permit more precise control.

The Request/Response objects and properties are mostly undocumented, as they line up exactly with the
protocol fields documented by Kafka at https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
*/
package sarama

import (
	"io/ioutil"
	"log"
)

// Logger is the instance of golang's log.Logger that Sarama writes connection
// management events to. By default it is set to discard all log messages via ioutil.Discard,
// but you can set it to redirect wherever you want.
var Logger = log.New(ioutil.Discard, "[Sarama] ", log.LstdFlags)

// PanicHandler is called for recovering from panics spawned internally to the library (and thus
// not recoverable by the caller's goroutine). Defaults to nil, which means panics are not recovered.
var PanicHandler func(interface{})

// MaxRequestSize is the maximum size (in bytes) of any request that Sarama will attempt to send. Trying
// to send a request larger than this will result in an EncodingError. The default of 100 MiB is aligned
// with Kafka's default `socket.request.max.bytes`, which is the largest request the broker will attempt
// to process.
var MaxRequestSize uint32 = 100 * 1024 * 1024
