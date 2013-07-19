package protocol

import "errors"

// AlreadyConnected is the error returned when calling Connect() on a Broker that is already connected.
var AlreadyConnected = errors.New("kafka: broker: already connected")

// NotConnected is the error returned when trying to send or call Close() on a Broker that is not connected.
var NotConnected = errors.New("kafka: broker: not connected")
