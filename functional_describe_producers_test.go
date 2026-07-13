//go:build functional

package sarama

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFuncDescribeProducers(t *testing.T) {
	checkKafkaVersion(t, "2.8.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.Producer.Idempotent = true
	config.Producer.RequiredAcks = WaitForAll
	config.Producer.Return.Successes = true
	config.Net.MaxOpenRequests = 1

	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer safeClose(t, client)

	// an idempotent producer is allocated a producer id on first send, which
	// the partition leader then reports as active producer state
	producer, err := NewSyncProducerFromClient(client)
	require.NoError(t, err)
	defer safeClose(t, producer)

	_, _, err = producer.SendMessage(&ProducerMessage{Topic: "test.1", Value: StringEncoder("describe-producers")})
	require.NoError(t, err)

	leader, err := client.Leader("test.1", 0)
	require.NoError(t, err)

	req := &DescribeProducersRequest{
		Topics: []DescribeProducersRequestTopic{
			{Name: "test.1", PartitionIndexes: []int32{0}},
		},
	}
	res, err := leader.DescribeProducers(req)
	require.NoError(t, err)

	require.Len(t, res.Topics, 1)
	assert.Equal(t, "test.1", res.Topics[0].Name)
	require.Len(t, res.Topics[0].Partitions, 1)

	partition := res.Topics[0].Partitions[0]
	assert.Equal(t, int32(0), partition.PartitionIndex)
	require.Equal(t, ErrNoError, partition.ErrorCode)
	require.NotEmpty(t, partition.ActiveProducers)

	state := partition.ActiveProducers[0]
	assert.GreaterOrEqual(t, state.ProducerID, int64(0))
	assert.GreaterOrEqual(t, state.LastSequence, int32(0))
}
