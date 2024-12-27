//go:build !functional

package sarama

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransitions(t *testing.T) {
	testError := errors.New("test")
	type testCase struct {
		transitions   []ProducerTxnStatusFlag
		expectedError error
	}
	testCases := []testCase{
		{
			transitions: []ProducerTxnStatusFlag{
				ProducerTxnFlagUninitialized,
				ProducerTxnFlagReady,
				ProducerTxnFlagInTransaction,
				ProducerTxnFlagEndTransaction | ProducerTxnFlagCommittingTransaction,
				ProducerTxnFlagReady,
			},
			expectedError: nil,
		},
		{
			transitions: []ProducerTxnStatusFlag{
				ProducerTxnFlagUninitialized,
				ProducerTxnFlagReady,
				ProducerTxnFlagInTransaction,
				ProducerTxnFlagEndTransaction | ProducerTxnFlagAbortingTransaction,
				ProducerTxnFlagReady,
			},
			expectedError: nil,
		},
		{
			transitions: []ProducerTxnStatusFlag{
				ProducerTxnFlagUninitialized,
				ProducerTxnFlagReady,
				ProducerTxnFlagInTransaction,
				ProducerTxnFlagEndTransaction,
				ProducerTxnFlagInError | ProducerTxnFlagAbortableError,
			},
			expectedError: testError,
		},
		{
			transitions: []ProducerTxnStatusFlag{
				ProducerTxnFlagInError | ProducerTxnFlagAbortableError,
				ProducerTxnFlagEndTransaction | ProducerTxnFlagAbortingTransaction,
				ProducerTxnFlagReady,
			},
			expectedError: nil,
		},
		{
			transitions: []ProducerTxnStatusFlag{
				ProducerTxnFlagInError | ProducerTxnFlagAbortableError,
				ProducerTxnFlagEndTransaction | ProducerTxnFlagCommittingTransaction,
			},
			expectedError: ErrTransitionNotAllowed,
		},
		{
			transitions: []ProducerTxnStatusFlag{
				ProducerTxnFlagInError | ProducerTxnFlagFatalError,
				ProducerTxnFlagEndTransaction | ProducerTxnFlagAbortingTransaction,
			},
			expectedError: ErrTransitionNotAllowed,
		},
	}
	for _, tc := range testCases {
		txnmgr := transactionManager{}
		txnmgr.status = tc.transitions[0]
		var lastError error
		for i := 1; i < len(tc.transitions); i++ {
			var baseErr error
			if tc.transitions[i]&ProducerTxnFlagInError != 0 {
				baseErr = testError
			}
			lastError = txnmgr.transitionTo(tc.transitions[i], baseErr)
		}
		require.Equal(t, tc.expectedError, lastError, tc)
	}
}

func TestTxnmgrInitProducerIdTxn(t *testing.T) {
	broker := NewMockBroker(t, 1)
	defer broker.Close()

	metadataLeader := new(MetadataResponse)
	metadataLeader.Version = 4
	metadataLeader.ControllerID = broker.brokerID
	metadataLeader.AddBroker(broker.Addr(), broker.BrokerID())
	broker.Returns(metadataLeader)

	config := NewTestConfig()
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "test"
	config.Version = V0_11_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1

	client, err := NewClient([]string{broker.Addr()}, config)
	require.NoError(t, err)
	defer client.Close()

	findCoordinatorResponse := FindCoordinatorResponse{
		Coordinator: client.Brokers()[0],
		Err:         ErrNoError,
		Version:     1,
	}
	broker.Returns(&findCoordinatorResponse)

	producerIdResponse := &InitProducerIDResponse{
		Err:           ErrNoError,
		ProducerID:    1,
		ProducerEpoch: 0,
	}
	broker.Returns(producerIdResponse)

	txmng, err := newTransactionManager(config, client)
	require.NoError(t, err)

	require.Equal(t, int64(1), txmng.producerID)
	require.Equal(t, int16(0), txmng.producerEpoch)
	require.Equal(t, ProducerTxnFlagReady, txmng.status)
}

// TestTxnmgrInitProducerIdTxnCoordinatorLoading ensure we retry initProducerId when either FindCoordinator or InitProducerID returns ErrOffsetsLoadInProgress
func TestTxnmgrInitProducerIdTxnCoordinatorLoading(t *testing.T) {
	config := NewTestConfig()
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "txid-group"
	config.Version = V0_11_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1

	broker := NewMockBroker(t, 1)
	defer broker.Close()

	broker.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": NewMockMetadataResponse(t).
			SetController(broker.BrokerID()).
			SetBroker(broker.Addr(), broker.BrokerID()),
		"FindCoordinatorRequest": NewMockSequence(
			NewMockFindCoordinatorResponse(t).
				SetError(CoordinatorTransaction, "txid-group", ErrOffsetsLoadInProgress),
			NewMockFindCoordinatorResponse(t).
				SetError(CoordinatorTransaction, "txid-group", ErrOffsetsLoadInProgress),
			NewMockFindCoordinatorResponse(t).
				SetCoordinator(CoordinatorTransaction, "txid-group", broker),
		),
		"InitProducerIDRequest": NewMockSequence(
			NewMockInitProducerIDResponse(t).
				SetError(ErrOffsetsLoadInProgress),
			NewMockInitProducerIDResponse(t).
				SetError(ErrOffsetsLoadInProgress),
			NewMockInitProducerIDResponse(t).
				SetProducerID(1).
				SetProducerEpoch(0),
		),
	})

	client, err := NewClient([]string{broker.Addr()}, config)
	require.NoError(t, err)
	defer client.Close()

	txmng, err := newTransactionManager(config, client)
	require.NoError(t, err)

	require.Equal(t, int64(1), txmng.producerID)
	require.Equal(t, int16(0), txmng.producerEpoch)
	require.Equal(t, ProducerTxnFlagReady, txmng.status)
}

func TestMaybeAddPartitionToCurrentTxn(t *testing.T) {
	type testCase struct {
		initialFlags                         ProducerTxnStatusFlag
		initialPartitionsInCurrentTxn        topicPartitionSet
		initialPendingPartitionsInCurrentTxn topicPartitionSet

		tpToAdd map[string][]int32

		expectedPendingPartitions topicPartitionSet
		expectedPartitionsInTxn   topicPartitionSet
	}
	testCases := []testCase{
		{
			initialFlags: ProducerTxnFlagInTransaction,
			initialPartitionsInCurrentTxn: topicPartitionSet{
				{topic: "test-topic", partition: 0}: struct{}{},
			},
			initialPendingPartitionsInCurrentTxn: topicPartitionSet{},
			tpToAdd: map[string][]int32{
				"test-topic": {
					0,
				},
			},
			expectedPendingPartitions: topicPartitionSet{},
			expectedPartitionsInTxn: topicPartitionSet{
				{topic: "test-topic", partition: 0}: struct{}{},
			},
		},
		{
			initialFlags:                         ProducerTxnFlagInTransaction,
			initialPartitionsInCurrentTxn:        topicPartitionSet{},
			initialPendingPartitionsInCurrentTxn: topicPartitionSet{},
			tpToAdd: map[string][]int32{
				"test-topic": {
					0,
				},
			},
			expectedPendingPartitions: topicPartitionSet{
				{topic: "test-topic", partition: 0}: struct{}{},
			},
			expectedPartitionsInTxn: topicPartitionSet{},
		},
		{
			initialFlags: ProducerTxnFlagInTransaction,
			initialPartitionsInCurrentTxn: topicPartitionSet{
				{topic: "test-topic", partition: 0}: struct{}{},
			},
			initialPendingPartitionsInCurrentTxn: topicPartitionSet{},
			tpToAdd: map[string][]int32{
				"test-topic": {
					0,
				},
			},
			expectedPendingPartitions: topicPartitionSet{},
			expectedPartitionsInTxn: topicPartitionSet{
				{topic: "test-topic", partition: 0}: struct{}{},
			},
		},
		{
			initialFlags:                  ProducerTxnFlagInTransaction,
			initialPartitionsInCurrentTxn: topicPartitionSet{},
			initialPendingPartitionsInCurrentTxn: topicPartitionSet{
				{topic: "test-topic", partition: 0}: struct{}{},
			},
			tpToAdd: map[string][]int32{
				"test-topic": {
					0,
				},
			},
			expectedPendingPartitions: topicPartitionSet{
				{topic: "test-topic", partition: 0}: struct{}{},
			},
			expectedPartitionsInTxn: topicPartitionSet{},
		},
		{
			initialFlags:                         ProducerTxnFlagInError,
			initialPartitionsInCurrentTxn:        topicPartitionSet{},
			initialPendingPartitionsInCurrentTxn: topicPartitionSet{},
			tpToAdd: map[string][]int32{
				"test-topic": {
					0,
				},
			},
			expectedPendingPartitions: topicPartitionSet{},
			expectedPartitionsInTxn:   topicPartitionSet{},
		},
	}

	broker := NewMockBroker(t, 1)
	defer broker.Close()

	metadataLeader := new(MetadataResponse)
	metadataLeader.Version = 4
	metadataLeader.ControllerID = broker.brokerID
	metadataLeader.AddBroker(broker.Addr(), broker.BrokerID())
	metadataLeader.AddTopic("test-topic", ErrNoError)
	metadataLeader.AddTopicPartition("test-topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)

	config := NewTestConfig()
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "test"
	config.Version = V0_11_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1
	config.Producer.Transaction.Retry.Max = 0
	config.Producer.Transaction.Retry.Backoff = 0

	for _, tc := range testCases {
		func() {
			broker.Returns(metadataLeader)

			client, err := NewClient([]string{broker.Addr()}, config)
			require.NoError(t, err)
			defer client.Close()

			findCoordinatorResponse := FindCoordinatorResponse{
				Coordinator: client.Brokers()[0],
				Err:         ErrNoError,
				Version:     1,
			}
			broker.Returns(&findCoordinatorResponse)

			producerIdResponse := &InitProducerIDResponse{
				Err:           ErrNoError,
				ProducerID:    1,
				ProducerEpoch: 0,
			}
			broker.Returns(producerIdResponse)

			txmng, err := newTransactionManager(config, client)
			require.NoError(t, err)

			txmng.partitionsInCurrentTxn = tc.initialPartitionsInCurrentTxn
			txmng.pendingPartitionsInCurrentTxn = tc.initialPendingPartitionsInCurrentTxn
			txmng.status = tc.initialFlags

			for topic, partitions := range tc.tpToAdd {
				for _, partition := range partitions {
					txmng.maybeAddPartitionToCurrentTxn(topic, partition)
				}
			}
			require.Equal(t, tc.expectedPartitionsInTxn, txmng.partitionsInCurrentTxn, tc)
			require.Equal(t, tc.expectedPendingPartitions, txmng.pendingPartitionsInCurrentTxn, tc)
		}()
	}
}

func TestAddOffsetsToTxn(t *testing.T) {
	type testCase struct {
		brokerErr     KError
		initialFlags  ProducerTxnStatusFlag
		expectedFlags ProducerTxnStatusFlag
		expectedError error
		newOffsets    topicPartitionOffsets
	}

	originalOffsets := topicPartitionOffsets{
		topicPartition{topic: "test-topic", partition: 0}: {
			Partition: 0,
			Offset:    0,
		},
	}

	testCases := []testCase{
		{
			brokerErr:     ErrNoError,
			initialFlags:  ProducerTxnFlagInTransaction,
			expectedFlags: ProducerTxnFlagInTransaction,
			expectedError: nil,
			newOffsets:    topicPartitionOffsets{},
		},
		{
			brokerErr:     ErrConsumerCoordinatorNotAvailable,
			initialFlags:  ProducerTxnFlagInTransaction,
			expectedFlags: ProducerTxnFlagInTransaction,
			expectedError: ErrConsumerCoordinatorNotAvailable,
			newOffsets:    originalOffsets,
		},
		{
			brokerErr:     ErrNotCoordinatorForConsumer,
			initialFlags:  ProducerTxnFlagInTransaction,
			expectedFlags: ProducerTxnFlagInTransaction,
			expectedError: ErrNotCoordinatorForConsumer,
			newOffsets:    originalOffsets,
		},
		{
			brokerErr:     ErrOffsetsLoadInProgress,
			initialFlags:  ProducerTxnFlagInTransaction,
			expectedFlags: ProducerTxnFlagInTransaction,
			expectedError: ErrOffsetsLoadInProgress,
			newOffsets:    originalOffsets,
		},
		{
			brokerErr:     ErrConcurrentTransactions,
			initialFlags:  ProducerTxnFlagInTransaction,
			expectedFlags: ProducerTxnFlagInTransaction,
			expectedError: ErrConcurrentTransactions,
			newOffsets:    originalOffsets,
		},
		{
			brokerErr:     ErrUnknownProducerID,
			initialFlags:  ProducerTxnFlagInTransaction,
			expectedFlags: ProducerTxnFlagFatalError,
			expectedError: ErrUnknownProducerID,
			newOffsets:    originalOffsets,
		},
		{
			brokerErr:     ErrInvalidProducerIDMapping,
			initialFlags:  ProducerTxnFlagInTransaction,
			expectedFlags: ProducerTxnFlagFatalError,
			expectedError: ErrInvalidProducerIDMapping,
			newOffsets:    originalOffsets,
		},
		{
			brokerErr:     ErrGroupAuthorizationFailed,
			initialFlags:  ProducerTxnFlagInTransaction,
			expectedFlags: ProducerTxnFlagAbortableError,
			expectedError: ErrGroupAuthorizationFailed,
			newOffsets:    originalOffsets,
		},
	}

	broker := NewMockBroker(t, 1)
	defer broker.Close()

	metadataLeader := new(MetadataResponse)
	metadataLeader.Version = 4
	metadataLeader.ControllerID = broker.brokerID
	metadataLeader.AddBroker(broker.Addr(), broker.BrokerID())
	metadataLeader.AddTopic("test-topic", ErrNoError)
	metadataLeader.AddTopicPartition("test-topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)

	config := NewTestConfig()
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "test"
	config.Version = V0_11_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1
	config.Producer.Transaction.Retry.Max = 0
	config.Producer.Transaction.Retry.Backoff = 0

	offsets := topicPartitionOffsets{
		topicPartition{topic: "test-topic", partition: 0}: {
			Partition: 0,
			Offset:    0,
		},
	}

	for _, tc := range testCases {
		func() {
			broker.Returns(metadataLeader)

			client, err := NewClient([]string{broker.Addr()}, config)
			require.NoError(t, err)

			defer client.Close()

			findCoordinatorResponse := FindCoordinatorResponse{
				Coordinator: client.Brokers()[0],
				Err:         ErrNoError,
				Version:     1,
			}
			broker.Returns(&findCoordinatorResponse)

			producerIdResponse := &InitProducerIDResponse{
				Err:           ErrNoError,
				ProducerID:    1,
				ProducerEpoch: 0,
			}
			broker.Returns(producerIdResponse)

			txmng, err := newTransactionManager(config, client)
			require.NoError(t, err)

			txmng.status = tc.initialFlags

			broker.Returns(&AddOffsetsToTxnResponse{
				Err: tc.brokerErr,
			})

			if errors.Is(tc.brokerErr, ErrRequestTimedOut) ||
				errors.Is(tc.brokerErr, ErrConsumerCoordinatorNotAvailable) ||
				errors.Is(tc.brokerErr, ErrNotCoordinatorForConsumer) {
				broker.Returns(&FindCoordinatorResponse{
					Coordinator: client.Brokers()[0],
					Err:         ErrNoError,
					Version:     1,
				})
			}

			if tc.brokerErr == ErrNoError {
				broker.Returns(&FindCoordinatorResponse{
					Coordinator: client.Brokers()[0],
					Err:         ErrNoError,
					Version:     1,
				})
				broker.Returns(&TxnOffsetCommitResponse{
					Topics: map[string][]*PartitionError{
						"test-topic": {
							{
								Partition: 0,
								Err:       ErrNoError,
							},
						},
					},
				})
			}

			newOffsets, err := txmng.publishOffsetsToTxn(offsets, "test-group")
			if tc.expectedError != nil {
				require.Equal(t, tc.expectedError.Error(), err.Error())
			} else {
				require.Equal(t, tc.expectedError, err)
			}
			require.Equal(t, tc.newOffsets, newOffsets)
			require.True(t, tc.expectedFlags&txmng.status != 0)
		}()
	}
}

func TestTxnOffsetsCommit(t *testing.T) {
	type testCase struct {
		brokerErr       KError
		initialFlags    ProducerTxnStatusFlag
		initialOffsets  topicPartitionOffsets
		expectedFlags   ProducerTxnStatusFlag
		expectedError   error
		expectedOffsets topicPartitionOffsets
	}

	originalOffsets := topicPartitionOffsets{
		topicPartition{topic: "test-topic", partition: 0}: {
			Partition: 0,
			Offset:    0,
		},
	}

	testCases := []testCase{
		{
			brokerErr:    ErrConsumerCoordinatorNotAvailable,
			initialFlags: ProducerTxnFlagInTransaction,
			initialOffsets: topicPartitionOffsets{
				topicPartition{topic: "test-topic", partition: 0}: {
					Partition: 0,
					Offset:    0,
				},
			},
			expectedFlags:   ProducerTxnFlagInTransaction,
			expectedError:   Wrap(ErrTxnOffsetCommit, ErrConsumerCoordinatorNotAvailable),
			expectedOffsets: originalOffsets,
		},
		{
			brokerErr:    ErrNotCoordinatorForConsumer,
			initialFlags: ProducerTxnFlagInTransaction,
			initialOffsets: topicPartitionOffsets{
				topicPartition{topic: "test-topic", partition: 0}: {
					Partition: 0,
					Offset:    0,
				},
			},
			expectedFlags:   ProducerTxnFlagInTransaction,
			expectedError:   Wrap(ErrTxnOffsetCommit, ErrNotCoordinatorForConsumer),
			expectedOffsets: originalOffsets,
		},
		{
			brokerErr:    ErrNoError,
			initialFlags: ProducerTxnFlagInTransaction,
			initialOffsets: topicPartitionOffsets{
				topicPartition{topic: "test-topic", partition: 0}: {
					Partition: 0,
					Offset:    0,
				},
			},
			expectedFlags:   ProducerTxnFlagInTransaction,
			expectedError:   nil,
			expectedOffsets: topicPartitionOffsets{},
		},
		{
			brokerErr:    ErrUnknownTopicOrPartition,
			initialFlags: ProducerTxnFlagInTransaction,
			initialOffsets: topicPartitionOffsets{
				topicPartition{topic: "test-topic", partition: 0}: {
					Partition: 0,
					Offset:    0,
				},
			},
			expectedFlags:   ProducerTxnFlagInTransaction,
			expectedError:   Wrap(ErrTxnOffsetCommit, ErrUnknownTopicOrPartition),
			expectedOffsets: originalOffsets,
		},
		{
			brokerErr:    ErrOffsetsLoadInProgress,
			initialFlags: ProducerTxnFlagInTransaction,
			initialOffsets: topicPartitionOffsets{
				topicPartition{topic: "test-topic", partition: 0}: {
					Partition: 0,
					Offset:    0,
				},
			},
			expectedFlags:   ProducerTxnFlagInTransaction,
			expectedError:   Wrap(ErrTxnOffsetCommit, ErrOffsetsLoadInProgress),
			expectedOffsets: originalOffsets,
		},
		{
			brokerErr:    ErrIllegalGeneration,
			initialFlags: ProducerTxnFlagInTransaction,
			initialOffsets: topicPartitionOffsets{
				topicPartition{topic: "test-topic", partition: 0}: {
					Partition: 0,
					Offset:    0,
				},
			},
			expectedFlags:   ProducerTxnFlagAbortableError,
			expectedError:   ErrIllegalGeneration,
			expectedOffsets: originalOffsets,
		},
		{
			brokerErr:    ErrUnknownMemberId,
			initialFlags: ProducerTxnFlagInTransaction,
			initialOffsets: topicPartitionOffsets{
				topicPartition{topic: "test-topic", partition: 0}: {
					Partition: 0,
					Offset:    0,
				},
			},
			expectedFlags:   ProducerTxnFlagAbortableError,
			expectedError:   ErrUnknownMemberId,
			expectedOffsets: originalOffsets,
		},
		{
			brokerErr:    ErrFencedInstancedId,
			initialFlags: ProducerTxnFlagInTransaction,
			initialOffsets: topicPartitionOffsets{
				topicPartition{topic: "test-topic", partition: 0}: {
					Partition: 0,
					Offset:    0,
				},
			},
			expectedFlags:   ProducerTxnFlagAbortableError,
			expectedError:   ErrFencedInstancedId,
			expectedOffsets: originalOffsets,
		},
		{
			brokerErr:    ErrGroupAuthorizationFailed,
			initialFlags: ProducerTxnFlagInTransaction,
			initialOffsets: topicPartitionOffsets{
				topicPartition{topic: "test-topic", partition: 0}: {
					Partition: 0,
					Offset:    0,
				},
			},
			expectedFlags:   ProducerTxnFlagAbortableError,
			expectedError:   ErrGroupAuthorizationFailed,
			expectedOffsets: originalOffsets,
		},
		{
			brokerErr:    ErrKafkaStorageError,
			initialFlags: ProducerTxnFlagInTransaction,
			initialOffsets: topicPartitionOffsets{
				topicPartition{topic: "test-topic", partition: 0}: {
					Partition: 0,
					Offset:    0,
				},
			},
			expectedFlags:   ProducerTxnFlagFatalError,
			expectedError:   ErrKafkaStorageError,
			expectedOffsets: originalOffsets,
		},
	}

	broker := NewMockBroker(t, 1)
	defer broker.Close()

	config := NewTestConfig()
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "test"
	config.Version = V0_11_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1
	config.Producer.Transaction.Retry.Max = 0
	config.Producer.Transaction.Retry.Backoff = 0

	metadataLeader := new(MetadataResponse)
	metadataLeader.Version = 4
	metadataLeader.ControllerID = broker.brokerID
	metadataLeader.AddBroker(broker.Addr(), broker.BrokerID())
	metadataLeader.AddTopic("test-topic", ErrNoError)
	metadataLeader.AddTopicPartition("test-topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)

	for _, tc := range testCases {
		func() {
			broker.Returns(metadataLeader)

			client, err := NewClient([]string{broker.Addr()}, config)
			require.NoError(t, err)
			defer client.Close()

			findCoordinatorResponse := FindCoordinatorResponse{
				Coordinator: client.Brokers()[0],
				Err:         ErrNoError,
				Version:     1,
			}
			broker.Returns(&findCoordinatorResponse)

			producerIdResponse := &InitProducerIDResponse{
				Err:           ErrNoError,
				ProducerID:    1,
				ProducerEpoch: 0,
			}
			broker.Returns(producerIdResponse)

			txmng, err := newTransactionManager(config, client)
			require.NoError(t, err)

			txmng.status = tc.initialFlags

			broker.Returns(&AddOffsetsToTxnResponse{
				Err: ErrNoError,
			})

			broker.Returns(&FindCoordinatorResponse{
				Coordinator: client.Brokers()[0],
				Err:         ErrNoError,
				Version:     1,
			})
			broker.Returns(&TxnOffsetCommitResponse{
				Topics: map[string][]*PartitionError{
					"test-topic": {
						{
							Partition: 0,
							Err:       tc.brokerErr,
						},
					},
				},
			})
			if errors.Is(tc.brokerErr, ErrRequestTimedOut) ||
				errors.Is(tc.brokerErr, ErrConsumerCoordinatorNotAvailable) ||
				errors.Is(tc.brokerErr, ErrNotCoordinatorForConsumer) {
				broker.Returns(&FindCoordinatorResponse{
					Coordinator: client.Brokers()[0],
					Err:         ErrNoError,
					Version:     1,
				})
			}

			newOffsets, err := txmng.publishOffsetsToTxn(tc.initialOffsets, "test-group")
			if tc.expectedError != nil {
				require.Equal(t, tc.expectedError.Error(), err.Error())
			} else {
				require.Equal(t, tc.expectedError, err)
			}
			require.Equal(t, tc.expectedOffsets, newOffsets)
			require.True(t, tc.expectedFlags&txmng.status != 0)
		}()
	}
}

func TestEndTxn(t *testing.T) {
	type testCase struct {
		brokerErr     KError
		commit        bool
		expectedFlags ProducerTxnStatusFlag
		expectedError error
	}

	testCases := []testCase{
		{
			brokerErr:     ErrNoError,
			commit:        true,
			expectedFlags: ProducerTxnFlagReady,
			expectedError: nil,
		},
		{
			brokerErr:     ErrConsumerCoordinatorNotAvailable,
			commit:        true,
			expectedFlags: ProducerTxnFlagEndTransaction,
			expectedError: ErrConsumerCoordinatorNotAvailable,
		},
		{
			brokerErr:     ErrNotCoordinatorForConsumer,
			commit:        true,
			expectedFlags: ProducerTxnFlagEndTransaction,
			expectedError: ErrNotCoordinatorForConsumer,
		},
		{
			brokerErr:     ErrOffsetsLoadInProgress,
			commit:        true,
			expectedFlags: ProducerTxnFlagEndTransaction,
			expectedError: ErrOffsetsLoadInProgress,
		},
		{
			brokerErr:     ErrConcurrentTransactions,
			commit:        true,
			expectedFlags: ProducerTxnFlagEndTransaction,
			expectedError: ErrConcurrentTransactions,
		},
		{
			brokerErr:     ErrUnknownProducerID,
			commit:        true,
			expectedFlags: ProducerTxnFlagFatalError,
			expectedError: ErrUnknownProducerID,
		},
		{
			brokerErr:     ErrInvalidProducerIDMapping,
			commit:        true,
			expectedFlags: ProducerTxnFlagFatalError,
			expectedError: ErrInvalidProducerIDMapping,
		},
	}

	broker := NewMockBroker(t, 1)
	defer broker.Close()

	metadataLeader := new(MetadataResponse)
	metadataLeader.Version = 4
	metadataLeader.ControllerID = broker.brokerID
	metadataLeader.AddBroker(broker.Addr(), broker.BrokerID())
	metadataLeader.AddTopic("test-topic", ErrNoError)
	metadataLeader.AddTopicPartition("test-topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)

	config := NewTestConfig()
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "test"
	config.Version = V0_11_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1
	config.Producer.Transaction.Retry.Max = 0
	config.Producer.Transaction.Retry.Backoff = 0

	for _, tc := range testCases {
		func() {
			broker.Returns(metadataLeader)

			client, err := NewClient([]string{broker.Addr()}, config)
			require.NoError(t, err)
			defer client.Close()

			findCoordinatorResponse := FindCoordinatorResponse{
				Coordinator: client.Brokers()[0],
				Err:         ErrNoError,
				Version:     1,
			}
			broker.Returns(&findCoordinatorResponse)

			producerIdResponse := &InitProducerIDResponse{
				Err:           ErrNoError,
				ProducerID:    1,
				ProducerEpoch: 0,
			}
			broker.Returns(producerIdResponse)

			txmng, err := newTransactionManager(config, client)
			require.NoError(t, err)

			txmng.status = ProducerTxnFlagEndTransaction

			endTxnResponse := &EndTxnResponse{
				Err:          tc.brokerErr,
				ThrottleTime: 0,
			}
			broker.Returns(endTxnResponse)

			if errors.Is(tc.brokerErr, ErrRequestTimedOut) ||
				errors.Is(tc.brokerErr, ErrConsumerCoordinatorNotAvailable) ||
				errors.Is(tc.brokerErr, ErrNotCoordinatorForConsumer) {
				broker.Returns(&FindCoordinatorResponse{
					Coordinator: client.Brokers()[0],
					Err:         ErrNoError,
					Version:     1,
				})
			}

			err = txmng.endTxn(tc.commit)
			require.Equal(t, tc.expectedError, err)
			require.True(t, txmng.currentTxnStatus()&tc.expectedFlags != 0)
		}()
	}
}

func TestPublishPartitionToTxn(t *testing.T) {
	type testCase struct {
		brokerErr                 KError
		expectedFlags             ProducerTxnStatusFlag
		expectedError             error
		expectedPendingPartitions topicPartitionSet
		expectedPartitionsInTxn   topicPartitionSet
	}

	initialPendingTopicPartitionSet := topicPartitionSet{
		{
			topic:     "test-topic",
			partition: 0,
		}: struct{}{},
	}

	testCases := []testCase{
		{
			brokerErr:                 ErrNoError,
			expectedFlags:             ProducerTxnFlagInTransaction,
			expectedError:             nil,
			expectedPendingPartitions: topicPartitionSet{},
			expectedPartitionsInTxn:   initialPendingTopicPartitionSet,
		},
		{
			brokerErr:                 ErrConsumerCoordinatorNotAvailable,
			expectedFlags:             ProducerTxnFlagInTransaction,
			expectedError:             Wrap(ErrAddPartitionsToTxn, ErrConsumerCoordinatorNotAvailable),
			expectedPartitionsInTxn:   topicPartitionSet{},
			expectedPendingPartitions: initialPendingTopicPartitionSet,
		},
		{
			brokerErr:                 ErrNotCoordinatorForConsumer,
			expectedFlags:             ProducerTxnFlagInTransaction,
			expectedError:             Wrap(ErrAddPartitionsToTxn, ErrNotCoordinatorForConsumer),
			expectedPartitionsInTxn:   topicPartitionSet{},
			expectedPendingPartitions: initialPendingTopicPartitionSet,
		},
		{
			brokerErr:                 ErrUnknownTopicOrPartition,
			expectedFlags:             ProducerTxnFlagInTransaction,
			expectedError:             Wrap(ErrAddPartitionsToTxn, ErrUnknownTopicOrPartition),
			expectedPartitionsInTxn:   topicPartitionSet{},
			expectedPendingPartitions: initialPendingTopicPartitionSet,
		},
		{
			brokerErr:                 ErrOffsetsLoadInProgress,
			expectedFlags:             ProducerTxnFlagInTransaction,
			expectedError:             Wrap(ErrAddPartitionsToTxn, ErrOffsetsLoadInProgress),
			expectedPartitionsInTxn:   topicPartitionSet{},
			expectedPendingPartitions: initialPendingTopicPartitionSet,
		},
		{
			brokerErr:                 ErrConcurrentTransactions,
			expectedFlags:             ProducerTxnFlagInTransaction,
			expectedError:             Wrap(ErrAddPartitionsToTxn, ErrConcurrentTransactions),
			expectedPartitionsInTxn:   topicPartitionSet{},
			expectedPendingPartitions: initialPendingTopicPartitionSet,
		},
		{
			brokerErr:                 ErrOperationNotAttempted,
			expectedFlags:             ProducerTxnFlagAbortableError,
			expectedError:             ErrOperationNotAttempted,
			expectedPartitionsInTxn:   topicPartitionSet{},
			expectedPendingPartitions: topicPartitionSet{},
		},
		{
			brokerErr:                 ErrTopicAuthorizationFailed,
			expectedFlags:             ProducerTxnFlagAbortableError,
			expectedError:             ErrTopicAuthorizationFailed,
			expectedPartitionsInTxn:   topicPartitionSet{},
			expectedPendingPartitions: topicPartitionSet{},
		},
		{
			brokerErr:                 ErrUnknownProducerID,
			expectedFlags:             ProducerTxnFlagFatalError,
			expectedError:             ErrUnknownProducerID,
			expectedPartitionsInTxn:   topicPartitionSet{},
			expectedPendingPartitions: topicPartitionSet{},
		},
		{
			brokerErr:                 ErrInvalidProducerIDMapping,
			expectedFlags:             ProducerTxnFlagFatalError,
			expectedError:             ErrInvalidProducerIDMapping,
			expectedPartitionsInTxn:   topicPartitionSet{},
			expectedPendingPartitions: topicPartitionSet{},
		},
		{
			brokerErr:                 ErrKafkaStorageError,
			expectedFlags:             ProducerTxnFlagFatalError,
			expectedError:             ErrKafkaStorageError,
			expectedPartitionsInTxn:   topicPartitionSet{},
			expectedPendingPartitions: topicPartitionSet{},
		},
	}

	broker := NewMockBroker(t, 1)
	defer broker.Close()

	metadataLeader := new(MetadataResponse)
	metadataLeader.Version = 4
	metadataLeader.ControllerID = broker.brokerID
	metadataLeader.AddBroker(broker.Addr(), broker.BrokerID())
	metadataLeader.AddTopic("test-topic", ErrNoError)
	metadataLeader.AddTopicPartition("test-topic", 0, broker.BrokerID(), nil, nil, nil, ErrNoError)

	config := NewTestConfig()
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "test"
	config.Version = V0_11_0_0
	config.Producer.RequiredAcks = WaitForAll
	config.Net.MaxOpenRequests = 1
	config.Producer.Transaction.Retry.Max = 0
	config.Producer.Transaction.Retry.Backoff = 0

	for _, tc := range testCases {
		func() {
			broker.Returns(metadataLeader)

			client, err := NewClient([]string{broker.Addr()}, config)
			require.NoError(t, err)
			defer client.Close()

			findCoordinatorResponse := FindCoordinatorResponse{
				Coordinator: client.Brokers()[0],
				Err:         ErrNoError,
				Version:     1,
			}
			broker.Returns(&findCoordinatorResponse)

			producerIdResponse := &InitProducerIDResponse{
				Err:           ErrNoError,
				ProducerID:    1,
				ProducerEpoch: 0,
			}
			broker.Returns(producerIdResponse)

			txmng, err := newTransactionManager(config, client)
			require.NoError(t, err)

			txmng.status = ProducerTxnFlagInTransaction
			txmng.pendingPartitionsInCurrentTxn = topicPartitionSet{
				{
					topic:     "test-topic",
					partition: 0,
				}: struct{}{},
			}
			broker.Returns(&AddPartitionsToTxnResponse{
				Errors: map[string][]*PartitionError{
					"test-topic": {
						{
							Partition: 0,
							Err:       tc.brokerErr,
						},
					},
				},
			})
			if errors.Is(tc.brokerErr, ErrRequestTimedOut) ||
				errors.Is(tc.brokerErr, ErrConsumerCoordinatorNotAvailable) ||
				errors.Is(tc.brokerErr, ErrNotCoordinatorForConsumer) {
				broker.Returns(&FindCoordinatorResponse{
					Coordinator: client.Brokers()[0],
					Err:         ErrNoError,
					Version:     1,
				})
			}
			err = txmng.publishTxnPartitions()
			if tc.expectedError != nil {
				require.Equal(t, tc.expectedError.Error(), err.Error(), tc)
			} else {
				require.Equal(t, tc.expectedError, err, tc)
			}

			require.True(t, txmng.status&tc.expectedFlags != 0, tc)
			require.Equal(t, tc.expectedPartitionsInTxn, txmng.partitionsInCurrentTxn, tc)
			require.Equal(t, tc.expectedPendingPartitions, txmng.pendingPartitionsInCurrentTxn, tc)
		}()
	}
}
