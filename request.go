package kafka

type RequestType uint16

const (
	REQUEST_PRODUCE        RequestType = 0
	REQUEST_FETCH                      = 1
	REQUEST_OFFSET                     = 2
	REQUEST_METADATA                   = 3
	REQUEST_LEADER_AND_ISR             = 4
	REQUEST_STOP_REPLICA               = 5
	REQUEST_OFFSET_COMMIT              = 6
	REQUEST_OFFSET_FETCH               = 7
)

type ApiVersion uint16

var API_VERSIONS = map[RequestType]ApiVersion{
	REQUEST_PRODUCE:        0,
	REQUEST_FETCH:          0,
	REQUEST_OFFSET:         0,
	REQUEST_METADATA:       0,
	REQUEST_LEADER_AND_ISR: 0,
	REQUEST_STOP_REPLICA:   0,
	REQUEST_OFFSET_COMMIT:  0,
	REQUEST_OFFSET_FETCH:   0,
}
