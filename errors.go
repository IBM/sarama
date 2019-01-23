package sarama

import (
	"errors"
	"fmt"
)

// ErrOutOfBrokers is the error returned when the client has run out of brokers to talk to because all of them errored
// or otherwise failed to respond.
var ErrOutOfBrokers = errors.New("kafka: client has run out of available brokers to talk to (Is your cluster reachable?)")

// ErrClosedClient is the error returned when a method is called on a client that has been closed.
var ErrClosedClient = errors.New("kafka: tried to use a client that was closed")

// ErrIncompleteResponse is the error returned when the server returns a syntactically valid response, but it does
// not contain the expected information.
var ErrIncompleteResponse = errors.New("kafka: response did not contain all the expected topic/partition blocks")

// ErrInvalidPartition is the error returned when a partitioner returns an invalid partition index
// (meaning one outside of the range [0...numPartitions-1]).
var ErrInvalidPartition = errors.New("kafka: partitioner returned an invalid partition index")

// ErrAlreadyConnected is the error returned when calling Open() on a Broker that is already connected or connecting.
var ErrAlreadyConnected = errors.New("kafka: broker connection already initiated")

// ErrNotConnected is the error returned when trying to send or call Close() on a Broker that is not connected.
var ErrNotConnected = errors.New("kafka: broker not connected")

// ErrInsufficientData is returned when decoding and the packet is truncated. This can be expected
// when requesting messages, since as an optimization the server is allowed to return a partial message at the end
// of the message set.
var ErrInsufficientData = errors.New("kafka: insufficient data to decode packet, more bytes expected")

// ErrShuttingDown is returned when a producer receives a message during shutdown.
var ErrShuttingDown = errors.New("kafka: message received by producer in process of shutting down")

// ErrMessageSizeTooLarge is returned when the next message to consume is larger than the configured Consumer.Fetch.Max
var ErrMessageSizeTooLarge = errors.New("kafka: message is larger than Consumer.Fetch.Max")

// ErrConsumerOffsetNotAdvanced is returned when a partition consumer didn't advance its offset after parsing
// a RecordBatch.
var ErrConsumerOffsetNotAdvanced = errors.New("kafka: consumer offset was not advanced after a RecordBatch")

// ErrControllerNotAvailable is returned when server didn't give correct controller id. May be kafka server's version
// is lower than 0.10.0.0.
var ErrControllerNotAvailable = errors.New("kafka: controller is not available")

// ErrNoTopicsToUpdateMetadata is returned when Meta.Full is set to false but no specific topics were found to update
// the metadata.
var ErrNoTopicsToUpdateMetadata = errors.New("kafka: no specific topics to update metadata")

// PacketEncodingError is returned from a failure while encoding a Kafka packet. This can happen, for example,
// if you try to encode a string over 2^15 characters in length, since Kafka's encoding rules do not permit that.
type PacketEncodingError struct {
	Info string
}

func (err PacketEncodingError) Error() string {
	return fmt.Sprintf("kafka: error encoding packet: %s", err.Info)
}

// PacketDecodingError is returned when there was an error (other than truncated data) decoding the Kafka broker's response.
// This can be a bad CRC or length field, or any other invalid value.
type PacketDecodingError struct {
	Info string
}

func (err PacketDecodingError) Error() string {
	return fmt.Sprintf("kafka: error decoding packet: %s", err.Info)
}

// ConfigurationError is the type of error returned from a constructor (e.g. NewClient, or NewConsumer)
// when the specified configuration is invalid.
type ConfigurationError string

func (err ConfigurationError) Error() string {
	return "kafka: invalid configuration (" + string(err) + ")"
}

// KError is the type of error that can be returned directly by the Kafka broker.
// See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes
type KError int16

// Numeric error codes returned by the Kafka server.
const (
	ErrNoError                            KError = 0
	ErrUnknown                            KError = -1
	ErrOffsetOutOfRange                   KError = 1
	ErrCorruptMessage                     KError = 2
	ErrUnknownTopicOrPartition            KError = 3
	ErrInvalidFetchSize                   KError = 4
	ErrLeaderNotAvailable                 KError = 5
	ErrNotLeaderForPartition              KError = 6
	ErrRequestTimedOut                    KError = 7
	ErrBrokerNotAvailable                 KError = 8
	ErrReplicaNotAvailable                KError = 9
	ErrMessageTooLarge                    KError = 10
	ErrStaleControllerEpoch               KError = 11
	ErrOffsetMetadataTooLarge             KError = 12
	ErrNetworkException                   KError = 13
	ErrCoordinatorLoadInProgress          KError = 14
	ErrCoordinatorNotAvailable            KError = 15
	ErrNotCoordinator                     KError = 16
	ErrInvalidTopicException              KError = 17
	ErrRecordListTooLarge                 KError = 18
	ErrNotEnoughReplicas                  KError = 19
	ErrNotEnoughReplicasAfterAppend       KError = 20
	ErrInvalidRequiredAcks                KError = 21
	ErrIllegalGeneration                  KError = 22
	ErrInconsistentGroupProtocol          KError = 23
	ErrInvalidGroupID                     KError = 24
	ErrUnknownMemberID                    KError = 25
	ErrInvalidSessionTimeout              KError = 26
	ErrRebalanceInProgress                KError = 27
	ErrInvalidCommitOffsetSize            KError = 28
	ErrTopicAuthorizationFailed           KError = 29
	ErrGroupAuthorizationFailed           KError = 30
	ErrClusterAuthorizationFailed         KError = 31
	ErrInvalidTimestamp                   KError = 32
	ErrUnsupportedSASLMechanism           KError = 33
	ErrIllegalSASLState                   KError = 34
	ErrUnsupportedVersion                 KError = 35
	ErrTopicAlreadyExists                 KError = 36
	ErrInvalidPartitions                  KError = 37
	ErrInvalidReplicationFactor           KError = 38
	ErrInvalidReplicaAssignment           KError = 39
	ErrInvalidConfig                      KError = 40
	ErrNotController                      KError = 41
	ErrInvalidRequest                     KError = 42
	ErrUnsupportedForMessageFormat        KError = 43
	ErrPolicyViolation                    KError = 44
	ErrOutOfOrderSequenceNumber           KError = 45
	ErrDuplicateSequenceNumber            KError = 46
	ErrInvalidProducerEpoch               KError = 47
	ErrInvalidTxnState                    KError = 48
	ErrInvalidProducerIDMapping           KError = 49
	ErrInvalidTransactionTimeout          KError = 50
	ErrConcurrentTransactions             KError = 51
	ErrTransactionCoordinatorFenced       KError = 52
	ErrTransactionalIDAuthorizationFailed KError = 53
	ErrSecurityDisabled                   KError = 54
	ErrOperationNotAttempted              KError = 55
	ErrKafkaStorageError                  KError = 56
	ErrLogDirNotFound                     KError = 57
	ErrSASLAuthenticationFailed           KError = 58
	ErrUnknownProducerID                  KError = 59
	ErrReassignmentInProgress             KError = 60
	ErrDelegationTokenAuthDisabled        KError = 61
	ErrDelegationTokenNotFound            KError = 62
	ErrDelegationTokenOwnerMismatch       KError = 63
	ErrDelegationTokenRequestNotAllowed   KError = 64
	ErrDelegationTokenAuthorizationFailed KError = 65
	ErrDelegationTokenExpired             KError = 66
	ErrInvalidPrincipalType               KError = 67
	ErrNonEmptyGroup                      KError = 68
	ErrGroupIDNotFound                    KError = 69
	ErrFetchSessionIDNotFound             KError = 70
	ErrInvalidFetchSessionEpoch           KError = 71
	ErrListenerNotFound                   KError = 72
)

func (err KError) Error() string {
	// Error messages stolen/adapted from
	// https://kafka.apache.org/protocol#protocol_error_codes
	switch err {
	case ErrNoError:
		return "kafka server: Not an error, why are you printing me?"
	case ErrUnknown:
		return "kafka server: Unexpected (unknown?) server error."
	case ErrOffsetOutOfRange:
		return "kafka server: The requested offset is not within the range of offsets maintained by the server."
	case ErrCorruptMessage:
		return "kafka server: This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt."
	case ErrUnknownTopicOrPartition:
		return "kafka server: This server does not host this topic-partition."
	case ErrInvalidFetchSize:
		return "kafka server: The requested fetch size is invalid."
	case ErrLeaderNotAvailable:
		return "kafka server: There is no leader for this topic-partition as we are in the middle of a leadership election."
	case ErrNotLeaderForPartition:
		return "kafka server: This server is not the leader for that topic-partition."
	case ErrRequestTimedOut:
		return "kafka server: The request timed out."
	case ErrBrokerNotAvailable:
		return "kafka server: The broker is not available."
	case ErrReplicaNotAvailable:
		return "kafka server: The replica is not available for the requested topic-partition."
	case ErrMessageTooLarge:
		return "kafka server: The request included a message larger than the max message size the server will accept."
	case ErrStaleControllerEpoch:
		return "kafka server: The controller moved to another broker."
	case ErrOffsetMetadataTooLarge:
		return "kafka server: The metadata field of the offset request was too large."
	case ErrNetworkException:
		return "kafka server: The server disconnected before a response was received."
	case ErrCoordinatorLoadInProgress:
		return "kafka server: The coordinator is loading and hence can't process requests."
	case ErrCoordinatorNotAvailable:
		return "kafka server: The coordinator is not available."
	case ErrNotCoordinator:
		return "kafka server: This is not the correct coordinator."
	case ErrInvalidTopicException:
		return "kafka server: The request attempted to perform an operation on an invalid topic."
	case ErrRecordListTooLarge:
		return "kafka server: The request included message batch larger than the configured segment size on the server."
	case ErrNotEnoughReplicas:
		return "kafka server: Messages are rejected since there are fewer in-sync replicas than required."
	case ErrNotEnoughReplicasAfterAppend:
		return "kafka server: Messages are written to the log, but to fewer in-sync replicas than required."
	case ErrInvalidRequiredAcks:
		return "kafka server: Produce request specified an invalid value for required acks."
	case ErrIllegalGeneration:
		return "kafka server: Specified group generation id is not valid."
	case ErrInconsistentGroupProtocol:
		return "kafka server: The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list."
	case ErrInvalidGroupID:
		return "kafka server: The configured groupId is invalid."
	case ErrUnknownMemberID:
		return "kafka server: The coordinator is not aware of this member."
	case ErrInvalidSessionTimeout:
		return "kafka server: The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms)."
	case ErrRebalanceInProgress:
		return "kafka server: The group is rebalancing, so a rejoin is needed."
	case ErrInvalidCommitOffsetSize:
		return "kafka server: The committing offset data size is not valid."
	case ErrTopicAuthorizationFailed:
		return "kafka server: Not authorized to access topics: [Topic authorization failed.]"
	case ErrGroupAuthorizationFailed:
		return "kafka server: Not authorized to access group: Group authorization failed."
	case ErrClusterAuthorizationFailed:
		return "kafka server: Cluster authorization failed."
	case ErrInvalidTimestamp:
		return "kafka server: The timestamp of the message is out of acceptable range."
	case ErrUnsupportedSASLMechanism:
		return "kafka server: The broker does not support the requested SASL mechanism."
	case ErrIllegalSASLState:
		return "kafka server: Request is not valid given the current SASL state."
	case ErrUnsupportedVersion:
		return "kafka server: The version of API is not supported."
	case ErrTopicAlreadyExists:
		return "kafka server: Topic with this name already exists."
	case ErrInvalidPartitions:
		return "kafka server: Number of partitions is below 1."
	case ErrInvalidReplicationFactor:
		return "kafka server: Replication factor is below 1 or larger than the number of available brokers."
	case ErrInvalidReplicaAssignment:
		return "kafka server: Replica assignment is invalid."
	case ErrInvalidConfig:
		return "kafka server: Configuration is invalid."
	case ErrNotController:
		return "kafka server: This is not the correct controller for this cluster."
	case ErrInvalidRequest:
		return "kafka server: This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details."
	case ErrUnsupportedForMessageFormat:
		return "kafka server: The message format version on the broker does not support the request."
	case ErrPolicyViolation:
		return "kafka server: Request parameters do not satisfy the configured policy."
	case ErrOutOfOrderSequenceNumber:
		return "kafka server: The broker received an out of order sequence number."
	case ErrDuplicateSequenceNumber:
		return "kafka server: The broker received a duplicate sequence number."
	case ErrInvalidProducerEpoch:
		return "kafka server: Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker."
	case ErrInvalidTxnState:
		return "kafka server: The producer attempted a transactional operation in an invalid state."
	case ErrInvalidProducerIDMapping:
		return "kafka server: The producer attempted to use a producer id which is not currently assigned to its transactional id."
	case ErrInvalidTransactionTimeout:
		return "kafka server: The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms)."
	case ErrConcurrentTransactions:
		return "kafka server: The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing."
	case ErrTransactionCoordinatorFenced:
		return "kafka server: Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer."
	case ErrTransactionalIDAuthorizationFailed:
		return "kafka server: Transactional Id authorization failed."
	case ErrSecurityDisabled:
		return "kafka server: Security features are disabled."
	case ErrOperationNotAttempted:
		return "kafka server: The broker did not attempt to execute this operation. This may happen for batched Rpcs where some operations in the batch failed, causing the broker to respond without trying the rest."
	case ErrKafkaStorageError:
		return "kafka server: Disk error when trying to access log file on the disk."
	case ErrLogDirNotFound:
		return "kafka server: The user-specified log directory is not found in the broker config."
	case ErrSASLAuthenticationFailed:
		return "kafka server: SASL Authentication failed."
	case ErrUnknownProducerID:
		return "kafka server: This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception."
	case ErrReassignmentInProgress:
		return "kafka server: A partition reassignment is in progress."
	case ErrDelegationTokenAuthDisabled:
		return "kafka server: Delegation Token feature is not enabled."
	case ErrDelegationTokenNotFound:
		return "kafka server: Delegation Token is not found on server."
	case ErrDelegationTokenOwnerMismatch:
		return "kafka server: Specified Principal is not valid Owner/Renewer."
	case ErrDelegationTokenRequestNotAllowed:
		return "kafka server: Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels."
	case ErrDelegationTokenAuthorizationFailed:
		return "kafka server: Delegation Token authorization failed."
	case ErrDelegationTokenExpired:
		return "kafka server: Delegation Token is expired."
	case ErrInvalidPrincipalType:
		return "kafka server: Supplied principalType is not supported."
	case ErrNonEmptyGroup:
		return "kafka server: The group is not empty."
	case ErrGroupIDNotFound:
		return "kafka server: The group id does not exist."
	case ErrFetchSessionIDNotFound:
		return "kafka server: The fetch session ID was not found."
	case ErrInvalidFetchSessionEpoch:
		return "kafka server: The fetch session epoch is invalid."
	case ErrListenerNotFound:
		return "kafka server: There is no listener on the leader broker that matches the listener on which metadata request was processed."
	}

	return fmt.Sprintf("Unknown error, how did this happen? Error code = %d", err)
}
