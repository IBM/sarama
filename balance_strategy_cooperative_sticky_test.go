//go:build !functional

package sarama

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBalanceStrategyCooperativeSticky(t *testing.T) {
	const topic = "t"

	ownedMeta := func(generation int32, partitions ...int32) ConsumerGroupMemberMetadata {
		meta := ConsumerGroupMemberMetadata{
			Version:      2,
			Topics:       []string{topic},
			GenerationID: generation,
		}
		if len(partitions) > 0 {
			meta.OwnedPartitions = []*OwnedPartition{{Topic: topic, Partitions: partitions}}
		}
		return meta
	}

	t.Run("advertises the protocol name java recognizes", func(t *testing.T) {
		strategy := NewBalanceStrategyCooperativeSticky()
		require.Equal(t, "cooperative-sticky", strategy.Name())
	})

	t.Run("supports cooperative and eager", func(t *testing.T) {
		strategy := NewBalanceStrategyCooperativeSticky()
		protocol, err := selectRebalanceProtocol([]BalanceStrategy{strategy})
		require.NoError(t, err)
		require.Equal(t, RebalanceProtocolCooperative, protocol)
		protocol, err = selectRebalanceProtocol([]BalanceStrategy{strategy, NewBalanceStrategyRange()})
		require.NoError(t, err)
		require.Equal(t, RebalanceProtocolEager, protocol)
	})

	t.Run("the eager sticky strategy stays eager-only", func(t *testing.T) {
		sticky := NewBalanceStrategySticky()
		require.NotImplements(t, (*RebalanceProtocolBalanceStrategy)(nil), sticky, "sticky must not declare supported protocols")
		require.NotImplements(t, (*SubscriptionUserDataBalanceStrategy)(nil), sticky, "sticky must not provide subscription user data")
		require.NotImplements(t, (*OnAssignmentBalanceStrategy)(nil), sticky, "sticky must not observe assignments")
	})

	t.Run("attaches no assignment data", func(t *testing.T) {
		strategy := NewBalanceStrategyCooperativeSticky()
		data, err := strategy.AssignmentData("m1", map[string][]int32{topic: {0}}, 7)
		require.NoError(t, err)
		require.Nil(t, data)
	})

	t.Run("subscription user data is a bare int32 generation", func(t *testing.T) {
		strategy := NewBalanceStrategyCooperativeSticky().(*cooperativeStickyBalanceStrategy)

		data, err := strategy.SubscriptionUserData([]string{topic})
		require.NoError(t, err)
		require.Equal(t, []byte{0xff, 0xff, 0xff, 0xff}, data)

		strategy.OnAssignment(&ConsumerGroupMemberAssignment{}, 7)
		data, err = strategy.SubscriptionUserData([]string{topic})
		require.NoError(t, err)
		require.Equal(t, []byte{0x00, 0x00, 0x00, 0x07}, data)
	})

	t.Run("ownership is read from owned partitions not user data", func(t *testing.T) {
		meta := ownedMeta(7, 0, 3)
		data, err := memberData(meta, true)
		require.NoError(t, err)
		want := []topicPartitionAssignment{{Topic: topic, Partition: 0}, {Topic: topic, Partition: 3}}
		require.Equal(t, want, data.partitions())
		require.Equal(t, 7, data.generation())
	})

	t.Run("generation falls back to user data below subscription v2", func(t *testing.T) {
		userData, err := encode(&cooperativeStickyAssignorUserData{Generation: 4}, nil)
		require.NoError(t, err)
		meta := ConsumerGroupMemberMetadata{
			Version:         1, // no GenerationID field on the wire
			Topics:          []string{topic},
			UserData:        userData,
			OwnedPartitions: []*OwnedPartition{{Topic: topic, Partitions: []int32{1}}},
		}
		data, err := memberData(meta, true)
		require.NoError(t, err)
		require.Equal(t, 4, data.generation())
	})

	t.Run("a member reporting nothing owns nothing", func(t *testing.T) {
		data, err := memberData(ownedMeta(defaultGeneration), true)
		require.NoError(t, err)
		require.Empty(t, data.partitions())
		require.Equal(t, defaultGeneration, data.generation())
	})

	t.Run("unreadable user data does not fail the rebalance", func(t *testing.T) {
		meta := ownedMeta(defaultGeneration, 0)
		meta.UserData = []byte("invalid")
		plan, err := NewBalanceStrategyCooperativeSticky().Plan(
			map[string]ConsumerGroupMemberMetadata{"m1": meta},
			map[string][]int32{topic: {0}},
		)
		require.NoError(t, err)
		require.Equal(t, []int32{0}, planPartitions(plan, "m1", topic))
	})

	t.Run("partitions changing hands are withheld then reassigned", func(t *testing.T) {
		topics := map[string][]int32{topic: {0, 1, 2, 3}}

		strategy := NewBalanceStrategyCooperativeSticky()
		members := map[string]ConsumerGroupMemberMetadata{
			"m1": ownedMeta(1, 0, 1, 2, 3),
			"m2": ownedMeta(defaultGeneration),
		}
		plan, err := strategy.Plan(members, topics)
		require.NoError(t, err)

		require.Len(t, planPartitions(plan, "m1", topic), 2, "m1 should retain 2 partitions")
		require.Empty(t, planPartitions(plan, "m2", topic), "m2 should get nothing until m1 has revoked")

		retained := planPartitions(plan, "m1", topic)
		members = map[string]ConsumerGroupMemberMetadata{
			"m1": ownedMeta(2, retained...),
			"m2": ownedMeta(2),
		}
		plan, err = strategy.Plan(members, topics)
		require.NoError(t, err)
		require.Equal(t, retained, planPartitions(plan, "m1", topic), "m1 should keep its partitions across the rebalance")
		require.Len(t, planPartitions(plan, "m2", topic), 2, "m2 should get the 2 freed partitions")

		assigned := append(planPartitions(plan, "m1", topic), planPartitions(plan, "m2", topic)...)
		slices.Sort(assigned)
		require.Equal(t, []int32{0, 1, 2, 3}, assigned)
	})

	t.Run("unowned partitions are assigned immediately", func(t *testing.T) {
		strategy := NewBalanceStrategyCooperativeSticky()
		members := map[string]ConsumerGroupMemberMetadata{"m1": ownedMeta(defaultGeneration)}
		plan, err := strategy.Plan(members, map[string][]int32{topic: {0, 1}})
		require.NoError(t, err)
		require.Equal(t, []int32{0, 1}, planPartitions(plan, "m1", topic))
	})

	t.Run("an unchanged assignment is left alone", func(t *testing.T) {
		strategy := NewBalanceStrategyCooperativeSticky()
		members := map[string]ConsumerGroupMemberMetadata{
			"m1": ownedMeta(3, 0),
			"m2": ownedMeta(3, 1),
		}
		plan, err := strategy.Plan(members, map[string][]int32{topic: {0, 1}})
		require.NoError(t, err)
		require.Equal(t, []int32{0}, planPartitions(plan, "m1", topic))
		require.Equal(t, []int32{1}, planPartitions(plan, "m2", topic))
	})
}

func planPartitions(plan BalanceStrategyPlan, memberID, topic string) []int32 {
	partitions := slices.Clone(plan[memberID][topic])
	slices.Sort(partitions)
	return partitions
}
