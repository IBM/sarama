# Alter partition reassignments example

This example shows how to use `ClusterAdmin.AlterPartitionReassignments` to
change the replication factor of an existing topic, plus how to track the
reassignment to completion with `ListPartitionReassignments`.

```bash
$ go run main.go -brokers="127.0.0.1:9092" -topic my-topic -replication-factor 3
```

## What `replicas []int32` actually means

`AlterPartitionReassignments` takes one `[]int32` per partition. Each slice is
the **full target replica set** for that partition, not a delta against the
current set: the first entry is the preferred leader, and the rest are
followers, in order. Brokers omitted from the slice are removed; brokers added
to it are added.

To raise the replication factor without disturbing the current leader, read the
existing assignment first (`DescribeTopics`), then append additional broker IDs
to each partition's replica list:

```go
admin, _ := sarama.NewClusterAdmin(brokers, config)
defer admin.Close()

meta, _ := admin.DescribeTopics([]string{topic})
brokers, _, _ := admin.DescribeCluster()

target := make([][]int32, len(meta[0].Partitions))
for _, p := range meta[0].Partitions {
    replicas := append([]int32(nil), p.Replicas...)
    for _, b := range brokers {
        if !slices.Contains(replicas, b.ID()) && len(replicas) < targetRF {
            replicas = append(replicas, b.ID())
        }
    }
    target[p.ID] = replicas
}

if err := admin.AlterPartitionReassignments(topic, target); err != nil {
    log.Fatal(err)
}
```

The `[][]int32` argument is indexed by partition ID, so partition `N` must be
at index `N`. The example in `main.go` handles this plus uneven existing
distributions, and balances new replicas across the least-used brokers in the
cluster.

## Waiting for the move to complete

The call above only submits the request. The actual replica movement happens
in the background on the controller. Poll `ListPartitionReassignments` to
observe progress — the broker only returns partitions that are still moving,
so an empty response for the topic means the reassignment is done:

```go
for {
    status, _ := admin.ListPartitionReassignments(topic, partitions)
    if len(status[topic]) == 0 {
        break
    }
    time.Sleep(2 * time.Second)
}
```

## Flags

| flag | description |
| --- | --- |
| `-brokers` | Bootstrap brokers, comma separated |
| `-topic` | Topic whose partitions should be reassigned |
| `-replication-factor` | Target replication factor for every partition |
| `-version` | Kafka cluster version (must be 2.4 or newer; AlterPartitionReassignments was added in KIP-455) |
| `-poll-interval` | Interval between `ListPartitionReassignments` polls |
| `-poll-timeout` | Maximum time to wait for an in-progress reassignment to complete |
| `-verbose` | Enable sarama logging |
