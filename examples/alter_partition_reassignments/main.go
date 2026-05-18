package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

var (
	brokers           = ""
	topic             = ""
	replicationFactor = 0
	version           = ""
	pollInterval      = 2 * time.Second
	pollTimeout       = 2 * time.Minute
	verbose           = false
)

func init() {
	flag.StringVar(&brokers, "brokers", "", "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&topic, "topic", "", "Topic whose partitions should be reassigned")
	flag.IntVar(&replicationFactor, "replication-factor", 0, "Target replication factor for every partition")
	flag.StringVar(&version, "version", sarama.DefaultVersion.String(), "Kafka cluster version (must be 2.4 or newer)")
	flag.DurationVar(&pollInterval, "poll-interval", 2*time.Second, "Interval between ListPartitionReassignments polls")
	flag.DurationVar(&pollTimeout, "poll-timeout", 2*time.Minute, "Maximum time to wait for an in-progress reassignment to complete")
	flag.BoolVar(&verbose, "verbose", false, "Enable sarama logging")
	flag.Parse()

	if len(brokers) == 0 {
		panic("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}
	if len(topic) == 0 {
		panic("no topic given, please set the -topic flag")
	}
	if replicationFactor <= 0 {
		panic("invalid -replication-factor, must be > 0")
	}
}

func main() {
	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	kafkaVersion, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Fatalf("parse kafka version: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = kafkaVersion
	config.ClientID = "sarama-alter-partition-reassignments"

	admin, err := sarama.NewClusterAdmin(strings.Split(brokers, ","), config)
	if err != nil {
		log.Fatalf("create cluster admin: %v", err)
	}
	defer func() {
		if err := admin.Close(); err != nil {
			log.Printf("close cluster admin: %v", err)
		}
	}()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	current, err := describeTopic(admin, topic)
	if err != nil {
		log.Fatalf("describe topic %q: %v", topic, err)
	}
	log.Printf("current assignment for %q: %s", topic, formatAssignment(current))

	clusterBrokers, _, err := admin.DescribeCluster()
	if err != nil {
		log.Fatalf("describe cluster: %v", err)
	}
	if len(clusterBrokers) < replicationFactor {
		log.Fatalf("cluster has %d brokers, cannot satisfy replication factor %d", len(clusterBrokers), replicationFactor)
	}
	brokerIDs := make([]int32, 0, len(clusterBrokers))
	for _, b := range clusterBrokers {
		brokerIDs = append(brokerIDs, b.ID())
	}
	slices.Sort(brokerIDs)

	target, changed := buildTargetAssignment(current, brokerIDs, replicationFactor)
	if !changed {
		log.Printf("topic %q is already at replication factor %d, nothing to do", topic, replicationFactor)
		return
	}
	log.Printf("target assignment for %q:  %s", topic, formatAssignment(target))

	if err := admin.AlterPartitionReassignments(topic, target); err != nil {
		log.Fatalf("alter partition reassignments: %v", err)
	}
	log.Printf("submitted reassignment for %q, waiting for it to complete (timeout %s)", topic, pollTimeout)

	if err := waitForReassignment(ctx, admin, topic, partitionIDs(target), pollInterval, pollTimeout); err != nil {
		log.Fatalf("wait for reassignment: %v", err)
	}

	final, err := describeTopic(admin, topic)
	if err != nil {
		log.Fatalf("describe topic after reassignment: %v", err)
	}
	log.Printf("final assignment for %q:   %s", topic, formatAssignment(final))
}

// describeTopic returns the current replica set for every partition, indexed by
// partition ID. The inner slice mirrors what Kafka stores: an ordered list of
// broker IDs where the first entry is the preferred leader and the rest are
// followers.
func describeTopic(admin sarama.ClusterAdmin, name string) ([][]int32, error) {
	meta, err := admin.DescribeTopics([]string{name})
	if err != nil {
		return nil, err
	}
	if len(meta) == 0 {
		return nil, fmt.Errorf("topic %q not found", name)
	}
	t := meta[0]
	if t.Err != sarama.ErrNoError {
		return nil, t.Err
	}
	assignment := make([][]int32, len(t.Partitions))
	for _, p := range t.Partitions {
		if int(p.ID) >= len(assignment) {
			return nil, fmt.Errorf("unexpected partition id %d for topic %q with %d partitions", p.ID, name, len(t.Partitions))
		}
		assignment[p.ID] = slices.Clone(p.Replicas)
	}
	return assignment, nil
}

// buildTargetAssignment grows each partition's replica list to targetRF by
// appending the least-used broker IDs not already in the partition's list.
// Existing replicas are preserved in order so the preferred leader does not
// move. Partitions that are already at or above the target RF are passed
// through unchanged.
func buildTargetAssignment(current [][]int32, brokerIDs []int32, targetRF int) ([][]int32, bool) {
	usage := make(map[int32]int, len(brokerIDs))
	for _, replicas := range current {
		for _, id := range replicas {
			usage[id]++
		}
	}

	target := make([][]int32, len(current))
	changed := false
	for i, replicas := range current {
		target[i] = slices.Clone(replicas)
		for len(target[i]) < targetRF {
			id, ok := leastUsedBroker(brokerIDs, target[i], usage)
			if !ok {
				break
			}
			target[i] = append(target[i], id)
			usage[id]++
			changed = true
		}
	}
	return target, changed
}

// leastUsedBroker returns the broker in brokerIDs (sorted ascending) that is
// not already in exclude and currently hosts the fewest replicas, breaking
// ties by lower broker ID. ok is false if every broker is excluded.
func leastUsedBroker(brokerIDs, exclude []int32, usage map[int32]int) (int32, bool) {
	var best int32
	bestUsage := -1
	for _, id := range brokerIDs {
		if slices.Contains(exclude, id) {
			continue
		}
		if bestUsage == -1 || usage[id] < bestUsage {
			best = id
			bestUsage = usage[id]
		}
	}
	return best, bestUsage != -1
}

// waitForReassignment polls ListPartitionReassignments until the broker stops
// reporting any in-progress reassignment for the topic, or the timeout fires.
// Kafka only returns partitions that are actively being reassigned, so an
// empty TopicStatus entry means the move is complete.
func waitForReassignment(
	ctx context.Context,
	admin sarama.ClusterAdmin,
	topic string,
	partitions []int32,
	interval, timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		status, err := admin.ListPartitionReassignments(topic, partitions)
		if err != nil {
			return fmt.Errorf("list partition reassignments: %w", err)
		}
		inProgress := status[topic]
		if len(inProgress) == 0 {
			return nil
		}
		log.Printf("%d partition(s) still reassigning", len(inProgress))
		for p, s := range inProgress {
			log.Printf("  partition %d: adding=%v removing=%v current=%v", p, s.AddingReplicas, s.RemovingReplicas, s.Replicas)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for reassignment of %q to complete", topic)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func partitionIDs(assignment [][]int32) []int32 {
	ids := make([]int32, len(assignment))
	for i := range assignment {
		ids[i] = int32(i)
	}
	return ids
}

func formatAssignment(assignment [][]int32) string {
	parts := make([]string, len(assignment))
	for i, replicas := range assignment {
		parts[i] = fmt.Sprintf("p%d=%v", i, replicas)
	}
	return strings.Join(parts, " ")
}
