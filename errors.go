package kafka

type KError int16

const (
	NO_ERROR                    KError = 0
	UNKNOWN                            = -1
	OFFSET_OUT_OF_RANGE                = 1
	INVALID_MESSAGE                    = 2
	UNKNOWN_TOPIC_OR_PARTITION         = 3
	INVALID_MESSAGE_SIZE               = 4
	LEADER_NOT_AVAILABLE               = 5
	NOT_LEADER_FOR_PARTITION           = 6
	REQUEST_TIMED_OUT                  = 7
	BROKER_NOT_AVAILABLE               = 8
	REPLICA_NOT_AVAILABLE              = 9
	MESSAGE_SIZE_TOO_LARGE             = 10
	STALE_CONTROLLER_EPOCH_CODE        = 11
	OFFSET_METADATA_TOO_LARGE          = 12
)

func (err KError) Error() string {
	// Error messages stolen/adapted from
	// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
	switch err {
	case NO_ERROR:
		return "kafka server: Not an error, why are you printing me?"
	case UNKNOWN:
		return "kafka server: Unexpected (unknown?) server error."
	case OFFSET_OUT_OF_RANGE:
		return "kafka server: The requested offset is outside the range of offsets maintained by the server for the given topic/partition."
	case INVALID_MESSAGE:
		return "kafka server: Message contents does not match its CRC."
	case UNKNOWN_TOPIC_OR_PARTITION:
		return "kafka server: Request was for a topic or partition that does not exist on this broker."
	case INVALID_MESSAGE_SIZE:
		return "kafka server: The message has a negative size."
	case LEADER_NOT_AVAILABLE:
		return "kafka server: In the middle of a leadership election, there is currently no leader for this partition and hence it is unavailable for writes."
	case NOT_LEADER_FOR_PARTITION:
		return "kafka server: Tried to send a message to a replica that is not the leader for some partition. Your metadata is out of date."
	case REQUEST_TIMED_OUT:
		return "kafka server: Request exceeded the user-specified time limit in the request."
	case BROKER_NOT_AVAILABLE:
		return "kafka server: Broker not available. Not a client facing error, we should never receive this!!!"
	case REPLICA_NOT_AVAILABLE:
		return "kafka server: Replica not available. What is the difference between this and LeaderNotAvailable?"
	case MESSAGE_SIZE_TOO_LARGE:
		return "kafka server: Message was too large, server rejected it to avoid allocation error."
	case STALE_CONTROLLER_EPOCH_CODE:
		return "kafka server: Stale controller epoch code. ???"
	case OFFSET_METADATA_TOO_LARGE:
		return "kafka server: Specified a string larger than the configured maximum for offset metadata."
	default:
		return "Unknown error, how did this happen?"
	}
}

type OutOfBrokers struct {
}

func (err OutOfBrokers) Error() string {
	return "kafka: Client has run out of available brokers to talk to. Is your cluster reachable?"
}

type EncodingError struct {
}

func (err EncodingError) Error() string {
	return "kafka: Could not encode packet."
}

type DecodingError struct {
}

func (err DecodingError) Error() string {
	return "kafka: Could not decode packet. Is the server really speaking kafka?"
}
