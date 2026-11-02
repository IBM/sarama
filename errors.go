package sarama

import (
	"errors"
	"fmt"
	"strings"
)

// ErrOutOfBrokers is the error returned when the client has run out of brokers to talk to because all of them errored
// or otherwise failed to respond.
var ErrOutOfBrokers = errors.New("kafka: client has run out of available brokers to talk to")

// ErrBrokerNotFound is the error returned when there's no broker found for the requested ID.
var ErrBrokerNotFound = errors.New("kafka: broker for ID is not found")

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

// ErrMessageTooLarge is returned when the next message to consume is larger than the configured Consumer.Fetch.Max
var ErrMessageTooLarge = errors.New("kafka: message is larger than Consumer.Fetch.Max")

// ErrDecompressedBatchTooLarge is returned when a compressed record batch decompresses
// to more than the configured limit (see MaxDecompressedBatchSize).
var ErrDecompressedBatchTooLarge = errors.New("kafka: decompressed record batch is larger than the configured limit")

// ErrConsumerOffsetNotAdvanced is returned when a partition consumer didn't advance its offset after parsing
// a RecordBatch.
var ErrConsumerOffsetNotAdvanced = errors.New("kafka: consumer offset was not advanced after a RecordBatch")

// ErrConsumerRetriesExhausted is sent on a partition consumer's Errors channel
// when Consumer.Retry.Max consecutive dispatch attempts have failed. The
// partition consumer self-closes immediately after; in a consumer group this
// ends the session and triggers a fresh rejoin.
var ErrConsumerRetriesExhausted = errors.New("kafka: partition consumer giving up after consecutive failures")

// ErrControllerNotAvailable is returned when server didn't give correct controller id. May be kafka server's version
// is lower than 0.10.0.0.
var ErrControllerNotAvailable = errors.New("kafka: controller is not available")

// ErrNoTopicsToUpdateMetadata is returned when Meta.Full is set to false but no specific topics were found to update
// the metadata.
var ErrNoTopicsToUpdateMetadata = errors.New("kafka: no specific topics to update metadata")

// ErrUnknownScramMechanism is returned when user tries to AlterUserScramCredentials with unknown SCRAM mechanism
var ErrUnknownScramMechanism = errors.New("kafka: unknown SCRAM mechanism provided")

// ErrReassignPartitions is returned when altering partition assignments for a topic fails
var ErrReassignPartitions = errors.New("failed to reassign partitions for topic")

// ErrDeleteRecords is the type of error returned when fail to delete the required records
var ErrDeleteRecords = errors.New("kafka server: failed to delete records")

// ErrCreateACLs is the type of error returned when ACL creation failed
var ErrCreateACLs = errors.New("kafka server: failed to create one or more ACL rules")

// ErrAddPartitionsToTxn is returned when AddPartitionsToTxn failed multiple times
var ErrAddPartitionsToTxn = errors.New("transaction manager: failed to send partitions to transaction")

// ErrTxnOffsetCommit is returned when TxnOffsetCommit failed multiple times
var ErrTxnOffsetCommit = errors.New("transaction manager: failed to send offsets to transaction")

// ErrTransactionNotReady when transaction status is invalid for the current action.
var ErrTransactionNotReady = errors.New("transaction manager: transaction is not ready")

// ErrNonTransactedProducer when calling BeginTxn, CommitTxn or AbortTxn on a non transactional producer.
var ErrNonTransactedProducer = errors.New("transaction manager: you need to add TransactionalID to producer")

// ErrTransitionNotAllowed when txnmgr state transition is not valid.
var ErrTransitionNotAllowed = errors.New("transaction manager: invalid transition attempted")

// ErrCannotTransitionNilError when transition is attempted with an nil error.
var ErrCannotTransitionNilError = errors.New("transaction manager: cannot transition with a nil error")

// ErrTxnUnableToParseResponse when response is nil
var ErrTxnUnableToParseResponse = errors.New("transaction manager: unable to parse response")

// ErrUnknownMessage when the protocol message key is not recognized
var ErrUnknownMessage = errors.New("kafka: unknown protocol message key")

// MultiErrorFormat specifies the formatter applied to format multierrors.
//
// Deprecated: Please use [errors.Join] instead.
func MultiErrorFormat(es []error) string {
	if len(es) == 1 {
		return es[0].Error()
	}

	points := make([]string, len(es))
	for i, err := range es {
		points[i] = fmt.Sprintf("* %s", err)
	}

	return fmt.Sprintf(
		"%d errors occurred:\n\t%s\n",
		len(es), strings.Join(points, "\n\t"))
}

type sentinelError struct {
	sentinel error
	wrapped  error
}

func (err sentinelError) Error() string {
	if err.wrapped != nil {
		return fmt.Sprintf("%s: %v", err.sentinel, err.wrapped)
	} else {
		return fmt.Sprintf("%s", err.sentinel)
	}
}

func (err sentinelError) Is(target error) bool {
	return errors.Is(err.sentinel, target) || errors.Is(err.wrapped, target)
}

func (err sentinelError) Unwrap() error {
	return err.wrapped
}

func Wrap(sentinel error, wrapped ...error) sentinelError {
	return sentinelError{sentinel: sentinel, wrapped: errors.Join(wrapped...)}
}

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
	ErrUnknown                            KError = -1 // Errors.UNKNOWN_SERVER_ERROR
	ErrNoError                            KError = 0  // Errors.NONE
	ErrOffsetOutOfRange                   KError = 1  // Errors.OFFSET_OUT_OF_RANGE
	ErrInvalidMessage                     KError = 2  // Errors.CORRUPT_MESSAGE
	ErrUnknownTopicOrPartition            KError = 3  // Errors.UNKNOWN_TOPIC_OR_PARTITION
	ErrInvalidMessageSize                 KError = 4  // Errors.INVALID_FETCH_SIZE
	ErrLeaderNotAvailable                 KError = 5  // Errors.LEADER_NOT_AVAILABLE
	ErrNotLeaderForPartition              KError = 6  // Errors.NOT_LEADER_OR_FOLLOWER
	ErrRequestTimedOut                    KError = 7  // Errors.REQUEST_TIMED_OUT
	ErrBrokerNotAvailable                 KError = 8  // Errors.BROKER_NOT_AVAILABLE
	ErrReplicaNotAvailable                KError = 9  // Errors.REPLICA_NOT_AVAILABLE
	ErrMessageSizeTooLarge                KError = 10 // Errors.MESSAGE_TOO_LARGE
	ErrStaleControllerEpochCode           KError = 11 // Errors.STALE_CONTROLLER_EPOCH
	ErrOffsetMetadataTooLarge             KError = 12 // Errors.OFFSET_METADATA_TOO_LARGE
	ErrNetworkException                   KError = 13 // Errors.NETWORK_EXCEPTION
	ErrOffsetsLoadInProgress              KError = 14 // Errors.COORDINATOR_LOAD_IN_PROGRESS
	ErrConsumerCoordinatorNotAvailable    KError = 15 // Errors.COORDINATOR_NOT_AVAILABLE
	ErrNotCoordinatorForConsumer          KError = 16 // Errors.NOT_COORDINATOR
	ErrInvalidTopic                       KError = 17 // Errors.INVALID_TOPIC_EXCEPTION
	ErrMessageSetSizeTooLarge             KError = 18 // Errors.RECORD_LIST_TOO_LARGE
	ErrNotEnoughReplicas                  KError = 19 // Errors.NOT_ENOUGH_REPLICAS
	ErrNotEnoughReplicasAfterAppend       KError = 20 // Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND
	ErrInvalidRequiredAcks                KError = 21 // Errors.INVALID_REQUIRED_ACKS
	ErrIllegalGeneration                  KError = 22 // Errors.ILLEGAL_GENERATION
	ErrInconsistentGroupProtocol          KError = 23 // Errors.INCONSISTENT_GROUP_PROTOCOL
	ErrInvalidGroupId                     KError = 24 // Errors.INVALID_GROUP_ID
	ErrUnknownMemberId                    KError = 25 // Errors.UNKNOWN_MEMBER_ID
	ErrInvalidSessionTimeout              KError = 26 // Errors.INVALID_SESSION_TIMEOUT
	ErrRebalanceInProgress                KError = 27 // Errors.REBALANCE_IN_PROGRESS
	ErrInvalidCommitOffsetSize            KError = 28 // Errors.INVALID_COMMIT_OFFSET_SIZE
	ErrTopicAuthorizationFailed           KError = 29 // Errors.TOPIC_AUTHORIZATION_FAILED
	ErrGroupAuthorizationFailed           KError = 30 // Errors.GROUP_AUTHORIZATION_FAILED
	ErrClusterAuthorizationFailed         KError = 31 // Errors.CLUSTER_AUTHORIZATION_FAILED
	ErrInvalidTimestamp                   KError = 32 // Errors.INVALID_TIMESTAMP
	ErrUnsupportedSASLMechanism           KError = 33 // Errors.UNSUPPORTED_SASL_MECHANISM
	ErrIllegalSASLState                   KError = 34 // Errors.ILLEGAL_SASL_STATE
	ErrUnsupportedVersion                 KError = 35 // Errors.UNSUPPORTED_VERSION
	ErrTopicAlreadyExists                 KError = 36 // Errors.TOPIC_ALREADY_EXISTS
	ErrInvalidPartitions                  KError = 37 // Errors.INVALID_PARTITIONS
	ErrInvalidReplicationFactor           KError = 38 // Errors.INVALID_REPLICATION_FACTOR
	ErrInvalidReplicaAssignment           KError = 39 // Errors.INVALID_REPLICA_ASSIGNMENT
	ErrInvalidConfig                      KError = 40 // Errors.INVALID_CONFIG
	ErrNotController                      KError = 41 // Errors.NOT_CONTROLLER
	ErrInvalidRequest                     KError = 42 // Errors.INVALID_REQUEST
	ErrUnsupportedForMessageFormat        KError = 43 // Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT
	ErrPolicyViolation                    KError = 44 // Errors.POLICY_VIOLATION
	ErrOutOfOrderSequenceNumber           KError = 45 // Errors.OUT_OF_ORDER_SEQUENCE_NUMBER
	ErrDuplicateSequenceNumber            KError = 46 // Errors.DUPLICATE_SEQUENCE_NUMBER
	ErrInvalidProducerEpoch               KError = 47 // Errors.INVALID_PRODUCER_EPOCH
	ErrInvalidTxnState                    KError = 48 // Errors.INVALID_TXN_STATE
	ErrInvalidProducerIDMapping           KError = 49 // Errors.INVALID_PRODUCER_ID_MAPPING
	ErrInvalidTransactionTimeout          KError = 50 // Errors.INVALID_TRANSACTION_TIMEOUT
	ErrConcurrentTransactions             KError = 51 // Errors.CONCURRENT_TRANSACTIONS
	ErrTransactionCoordinatorFenced       KError = 52 // Errors.TRANSACTION_COORDINATOR_FENCED
	ErrTransactionalIDAuthorizationFailed KError = 53 // Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED
	ErrSecurityDisabled                   KError = 54 // Errors.SECURITY_DISABLED
	ErrOperationNotAttempted              KError = 55 // Errors.OPERATION_NOT_ATTEMPTED
	ErrKafkaStorageError                  KError = 56 // Errors.KAFKA_STORAGE_ERROR
	ErrLogDirNotFound                     KError = 57 // Errors.LOG_DIR_NOT_FOUND
	ErrSASLAuthenticationFailed           KError = 58 // Errors.SASL_AUTHENTICATION_FAILED
	ErrUnknownProducerID                  KError = 59 // Errors.UNKNOWN_PRODUCER_ID
	ErrReassignmentInProgress             KError = 60 // Errors.REASSIGNMENT_IN_PROGRESS
	ErrDelegationTokenAuthDisabled        KError = 61 // Errors.DELEGATION_TOKEN_AUTH_DISABLED
	ErrDelegationTokenNotFound            KError = 62 // Errors.DELEGATION_TOKEN_NOT_FOUND
	ErrDelegationTokenOwnerMismatch       KError = 63 // Errors.DELEGATION_TOKEN_OWNER_MISMATCH
	ErrDelegationTokenRequestNotAllowed   KError = 64 // Errors.DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
	ErrDelegationTokenAuthorizationFailed KError = 65 // Errors.DELEGATION_TOKEN_AUTHORIZATION_FAILED
	ErrDelegationTokenExpired             KError = 66 // Errors.DELEGATION_TOKEN_EXPIRED
	ErrInvalidPrincipalType               KError = 67 // Errors.INVALID_PRINCIPAL_TYPE
	ErrNonEmptyGroup                      KError = 68 // Errors.NON_EMPTY_GROUP
	ErrGroupIDNotFound                    KError = 69 // Errors.GROUP_ID_NOT_FOUND
	ErrFetchSessionIDNotFound             KError = 70 // Errors.FETCH_SESSION_ID_NOT_FOUND
	ErrInvalidFetchSessionEpoch           KError = 71 // Errors.INVALID_FETCH_SESSION_EPOCH
	ErrListenerNotFound                   KError = 72 // Errors.LISTENER_NOT_FOUND
	ErrTopicDeletionDisabled              KError = 73 // Errors.TOPIC_DELETION_DISABLED
	ErrFencedLeaderEpoch                  KError = 74 // Errors.FENCED_LEADER_EPOCH
	ErrUnknownLeaderEpoch                 KError = 75 // Errors.UNKNOWN_LEADER_EPOCH
	ErrUnsupportedCompressionType         KError = 76 // Errors.UNSUPPORTED_COMPRESSION_TYPE
	ErrStaleBrokerEpoch                   KError = 77 // Errors.STALE_BROKER_EPOCH
	ErrOffsetNotAvailable                 KError = 78 // Errors.OFFSET_NOT_AVAILABLE
	ErrMemberIdRequired                   KError = 79 // Errors.MEMBER_ID_REQUIRED
	ErrPreferredLeaderNotAvailable        KError = 80 // Errors.PREFERRED_LEADER_NOT_AVAILABLE
	ErrGroupMaxSizeReached                KError = 81 // Errors.GROUP_MAX_SIZE_REACHED
	ErrFencedInstancedId                  KError = 82 // Errors.FENCED_INSTANCE_ID
	ErrEligibleLeadersNotAvailable        KError = 83 // Errors.ELIGIBLE_LEADERS_NOT_AVAILABLE
	ErrElectionNotNeeded                  KError = 84 // Errors.ELECTION_NOT_NEEDED
	ErrNoReassignmentInProgress           KError = 85 // Errors.NO_REASSIGNMENT_IN_PROGRESS
	ErrGroupSubscribedToTopic             KError = 86 // Errors.GROUP_SUBSCRIBED_TO_TOPIC
	ErrInvalidRecord                      KError = 87 // Errors.INVALID_RECORD
	ErrUnstableOffsetCommit               KError = 88 // Errors.UNSTABLE_OFFSET_COMMIT
	ErrThrottlingQuotaExceeded            KError = 89 // Errors.THROTTLING_QUOTA_EXCEEDED
	ErrProducerFenced                     KError = 90 // Errors.PRODUCER_FENCED
	ErrInvalidUpdateVersion               KError = 95 // Errors.INVALID_UPDATE_VERSION
)

// kErrorMessages holds the broker-side description of each error code.
// Error messages stolen/adapted from
// https://kafka.apache.org/protocol#protocol_error_codes
var kErrorMessages = map[KError]string{
	ErrNoError:                            "Not an error, why are you printing me?",
	ErrUnknown:                            "Unexpected (unknown?) server error",
	ErrOffsetOutOfRange:                   "The requested offset is outside the range of offsets maintained by the server for the given topic/partition",
	ErrInvalidMessage:                     "Message contents does not match its CRC",
	ErrUnknownTopicOrPartition:            "Request was for a topic or partition that does not exist on this broker",
	ErrInvalidMessageSize:                 "The message has a negative size",
	ErrLeaderNotAvailable:                 "In the middle of a leadership election, there is currently no leader for this partition and hence it is unavailable for writes",
	ErrNotLeaderForPartition:              "Tried to send a message to a replica that is not the leader for some partition. Your metadata is out of date",
	ErrRequestTimedOut:                    "Request exceeded the user-specified time limit in the request",
	ErrBrokerNotAvailable:                 "Broker not available. Not a client facing error, we should never receive this!!!",
	ErrReplicaNotAvailable:                "Replica information not available, one or more brokers are down",
	ErrMessageSizeTooLarge:                "Message was too large, server rejected it to avoid allocation error",
	ErrStaleControllerEpochCode:           "StaleControllerEpochCode (internal error code for broker-to-broker communication)",
	ErrOffsetMetadataTooLarge:             "Specified a string larger than the configured maximum for offset metadata",
	ErrNetworkException:                   "The server disconnected before a response was received",
	ErrOffsetsLoadInProgress:              "The coordinator is still loading offsets and cannot currently process requests",
	ErrConsumerCoordinatorNotAvailable:    "The coordinator is not available",
	ErrNotCoordinatorForConsumer:          "Request was for a consumer group that is not coordinated by this broker",
	ErrInvalidTopic:                       "The request attempted to perform an operation on an invalid topic",
	ErrMessageSetSizeTooLarge:             "The request included message batch larger than the configured segment size on the server",
	ErrNotEnoughReplicas:                  "Messages are rejected since there are fewer in-sync replicas than required",
	ErrNotEnoughReplicasAfterAppend:       "Messages are written to the log, but to fewer in-sync replicas than required",
	ErrInvalidRequiredAcks:                "The number of required acks is invalid (should be either -1, 0, or 1)",
	ErrIllegalGeneration:                  "The provided generation id is not the current generation",
	ErrInconsistentGroupProtocol:          "The provider group protocol type is incompatible with the other members",
	ErrInvalidGroupId:                     "The provided group id was empty",
	ErrUnknownMemberId:                    "The provided member is not known in the current generation",
	ErrInvalidSessionTimeout:              "The provided session timeout is outside the allowed range",
	ErrRebalanceInProgress:                "A rebalance for the group is in progress. Please re-join the group",
	ErrInvalidCommitOffsetSize:            "The provided commit metadata was too large",
	ErrTopicAuthorizationFailed:           "The client is not authorized to access this topic",
	ErrGroupAuthorizationFailed:           "The client is not authorized to access this group",
	ErrClusterAuthorizationFailed:         "The client is not authorized to send this request type",
	ErrInvalidTimestamp:                   "The timestamp of the message is out of acceptable range",
	ErrUnsupportedSASLMechanism:           "The broker does not support the requested SASL mechanism",
	ErrIllegalSASLState:                   "Request is not valid given the current SASL state",
	ErrUnsupportedVersion:                 "The version of API is not supported",
	ErrTopicAlreadyExists:                 "Topic with this name already exists",
	ErrInvalidPartitions:                  "Number of partitions is invalid",
	ErrInvalidReplicationFactor:           "Replication-factor is invalid",
	ErrInvalidReplicaAssignment:           "Replica assignment is invalid",
	ErrInvalidConfig:                      "Configuration is invalid",
	ErrNotController:                      "This is not the correct controller for this cluster",
	ErrInvalidRequest:                     "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details",
	ErrUnsupportedForMessageFormat:        "The requested operation is not supported by the message format version",
	ErrPolicyViolation:                    "Request parameters do not satisfy the configured policy",
	ErrOutOfOrderSequenceNumber:           "The broker received an out of order sequence number",
	ErrDuplicateSequenceNumber:            "The broker received a duplicate sequence number",
	ErrInvalidProducerEpoch:               "Producer attempted an operation with an old epoch",
	ErrInvalidTxnState:                    "The producer attempted a transactional operation in an invalid state",
	ErrInvalidProducerIDMapping:           "The producer attempted to use a producer id which is not currently assigned to its transactional id",
	ErrInvalidTransactionTimeout:          "The transaction timeout is larger than the maximum value allowed by the broker (as configured by max.transaction.timeout.ms)",
	ErrConcurrentTransactions:             "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing",
	ErrTransactionCoordinatorFenced:       "The transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer",
	ErrTransactionalIDAuthorizationFailed: "Transactional ID authorization failed",
	ErrSecurityDisabled:                   "Security features are disabled",
	ErrOperationNotAttempted:              "The broker did not attempt to execute this operation",
	ErrKafkaStorageError:                  "Disk error when trying to access log file on the disk",
	ErrLogDirNotFound:                     "The specified log directory is not found in the broker config",
	ErrSASLAuthenticationFailed:           "SASL Authentication failed",
	ErrUnknownProducerID:                  "The broker could not locate the producer metadata associated with the Producer ID",
	ErrReassignmentInProgress:             "A partition reassignment is in progress",
	ErrDelegationTokenAuthDisabled:        "Delegation Token feature is not enabled",
	ErrDelegationTokenNotFound:            "Delegation Token is not found on server",
	ErrDelegationTokenOwnerMismatch:       "Specified Principal is not valid Owner/Renewer",
	ErrDelegationTokenRequestNotAllowed:   "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels",
	ErrDelegationTokenAuthorizationFailed: "Delegation Token authorization failed",
	ErrDelegationTokenExpired:             "Delegation Token is expired",
	ErrInvalidPrincipalType:               "Supplied principalType is not supported",
	ErrNonEmptyGroup:                      "The group is not empty",
	ErrGroupIDNotFound:                    "The group id does not exist",
	ErrFetchSessionIDNotFound:             "The fetch session ID was not found",
	ErrInvalidFetchSessionEpoch:           "The fetch session epoch is invalid",
	ErrListenerNotFound:                   "There is no listener on the leader broker that matches the listener on which metadata request was processed",
	ErrTopicDeletionDisabled:              "Topic deletion is disabled",
	ErrFencedLeaderEpoch:                  "The leader epoch in the request is older than the epoch on the broker",
	ErrUnknownLeaderEpoch:                 "The leader epoch in the request is newer than the epoch on the broker",
	ErrUnsupportedCompressionType:         "The requesting client does not support the compression type of given partition",
	ErrStaleBrokerEpoch:                   "Broker epoch has changed",
	ErrOffsetNotAvailable:                 "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing",
	ErrMemberIdRequired:                   "The group member needs to have a valid member id before actually entering a consumer group",
	ErrPreferredLeaderNotAvailable:        "The preferred leader was not available",
	ErrGroupMaxSizeReached:                "Consumer group The consumer group has reached its max size. already has the configured maximum number of members",
	ErrFencedInstancedId:                  "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id",
	ErrEligibleLeadersNotAvailable:        "Eligible topic partition leaders are not available",
	ErrElectionNotNeeded:                  "Leader election not needed for topic partition",
	ErrNoReassignmentInProgress:           "No partition reassignment is in progress",
	ErrGroupSubscribedToTopic:             "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it",
	ErrInvalidRecord:                      "This record has failed the validation on broker and hence will be rejected",
	ErrUnstableOffsetCommit:               "There are unstable offsets that need to be cleared",
	ErrThrottlingQuotaExceeded:            "The throttling quota has been exceeded",
	ErrInvalidUpdateVersion:               "The given update version was invalid",
}

func (err KError) Error() string {
	if msg, ok := kErrorMessages[err]; ok {
		return "kafka server: " + msg
	}
	return fmt.Sprintf("Unknown error, how did this happen? Error code = %d", err)
}

// isRetriableKError reports whether err maps to a RetriableException in the
// Java client, which resends a request that fails with one.
func isRetriableKError(err KError) bool {
	switch err {
	case ErrInvalidMessage, ErrUnknownTopicOrPartition, ErrLeaderNotAvailable,
		ErrNotLeaderForPartition, ErrRequestTimedOut, ErrReplicaNotAvailable,
		ErrNetworkException, ErrOffsetsLoadInProgress, ErrConsumerCoordinatorNotAvailable,
		ErrNotCoordinatorForConsumer, ErrNotEnoughReplicas, ErrNotEnoughReplicasAfterAppend,
		ErrNotController, ErrConcurrentTransactions, ErrKafkaStorageError,
		ErrFetchSessionIDNotFound, ErrInvalidFetchSessionEpoch, ErrListenerNotFound,
		ErrFencedLeaderEpoch, ErrUnknownLeaderEpoch, ErrOffsetNotAvailable,
		ErrPreferredLeaderNotAvailable, ErrEligibleLeadersNotAvailable, ErrElectionNotNeeded,
		ErrUnstableOffsetCommit, ErrThrottlingQuotaExceeded:
		return true
	}
	return false
}
