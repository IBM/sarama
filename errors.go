package kafka

type kafkaError int16

const (
	NO_ERROR                    kafkaError = 0
	UNKNOWN                                = -1
	OFFSET_OUT_OF_RANGE                    = 1
	INVALID_MESSAGE                        = 2
	UNKNOWN_TOPIC_OR_PARTITION             = 3
	INVALID_MESSAGE_SIZE                   = 4
	LEADER_NOT_AVAILABLE                   = 5
	NOT_LEADER_FOR_PARTITION               = 6
	REQUEST_TIMED_OUT                      = 7
	BROKER_NOT_AVAILABLE                   = 8
	REPLICA_NOT_AVAILABLE                  = 9
	MESSAGE_SIZE_TOO_LARGE                 = 10
	STALE_CONTROLLER_EPOCH_CODE            = 11
)
