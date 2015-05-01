package kazoo

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// Topic interacts with Kafka's topic metadata in Zookeeper.
type Topic struct {
	Name string
	kz   *Kazoo
}

// Partition interacts with Kafka's partition metadata in Zookeeper.
type Partition struct {
	topic    *Topic
	ID       int32
	Replicas []int32
}

// Topics returns a map of all registered Kafka topics.
func (kz *Kazoo) Topics() (map[string]*Topic, error) {
	root := fmt.Sprintf("%s/brokers/topics", kz.conf.Chroot)
	children, _, err := kz.conn.Children(root)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*Topic)
	for _, name := range children {
		result[name] = kz.Topic(name)
	}
	return result, nil
}

// Topic returns a Topic instance for a given topic name
func (kz *Kazoo) Topic(topic string) *Topic {
	return &Topic{Name: topic, kz: kz}
}

// Partitions returns a map of all partitions for the topic.
func (t *Topic) Partitions() (map[int32]*Partition, error) {
	node := fmt.Sprintf("%s/brokers/topics/%s", t.kz.conf.Chroot, t.Name)
	value, _, err := t.kz.conn.Get(node)
	if err != nil {
		return nil, err
	}

	type topicMetadata struct {
		Partitions map[string][]int32 `json:"partitions"`
	}

	var tm topicMetadata
	if err := json.Unmarshal(value, &tm); err != nil {
		return nil, err
	}

	result := make(map[int32]*Partition)
	for partitionNumber, replicas := range tm.Partitions {
		partitionID, err := strconv.ParseInt(partitionNumber, 10, 32)
		if err != nil {
			return nil, err
		}

		replicaIDs := make([]int32, 0, len(replicas))
		for _, r := range replicas {
			replicaIDs = append(replicaIDs, int32(r))
		}

		result[int32(partitionID)] = t.Partition(int32(partitionID), replicaIDs)
	}

	return result, nil
}

// Partition returns a Partition instance for the topic.
func (t *Topic) Partition(id int32, replicas []int32) *Partition {
	return &Partition{ID: id, Replicas: replicas, topic: t}
}

// Config returns topic-level configuration settings as a map.
func (t *Topic) Config() (map[string]string, error) {
	value, _, err := t.kz.conn.Get(fmt.Sprintf("%s/config/topics/%s", t.kz.conf.Chroot, t.Name))
	if err != nil {
		return nil, err
	}

	var topicConfig struct {
		ConfigMap map[string]string `json:"config"`
	}

	if err := json.Unmarshal(value, &topicConfig); err != nil {
		return nil, err
	}

	return topicConfig.ConfigMap, nil
}

// Leader returns the broker ID of the broker that is currently the leader for the partition.
func (p *Partition) Leader() (int32, error) {
	if state, err := p.state(); err != nil {
		return -1, err
	} else {
		return state.Leader, nil
	}
}

// ISR returns the broker IDs of the current in-sync replica set for the partition
func (p *Partition) ISR() ([]int32, error) {
	if state, err := p.state(); err != nil {
		return nil, err
	} else {
		return state.ISR, nil
	}
}

type partitionState struct {
	Leader int32   `json:"leader"`
	ISR    []int32 `json:"isr"`
}

func (p *Partition) state() (partitionState, error) {
	var state partitionState
	node := fmt.Sprintf("%s/brokers/topics/%s/partitions/%d/state", p.topic.kz.conf.Chroot, p.topic.Name, p.ID)
	value, _, err := p.topic.kz.conn.Get(node)
	if err != nil {
		return state, err
	}

	if err := json.Unmarshal(value, &state); err != nil {
		return state, err
	}

	return state, nil
}
