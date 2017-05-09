package sarama

type ReplicaAssignment struct {
	PartitionID int32
	Replicas    []int32
}

type ConfigKV struct {
	Key   string
	Value string
}

type CreateTopicRequest struct {
	Topic              string
	NumPartitions      int32
	ReplicationFactor  int16
	ReplicaAssignments []ReplicaAssignment
	Configs            []ConfigKV
}

type CreateTopicsRequest struct {
	CreateRequests []CreateTopicRequest
	Timeout        int32
}

func (cr *CreateTopicRequest) encode(pe packetEncoder) error {
	if err := pe.putString(cr.Topic); err != nil {
		return err
	}

	pe.putInt32(cr.NumPartitions)
	pe.putInt16(cr.ReplicationFactor)

	if err := pe.putArrayLength(len(cr.ReplicaAssignments)); err != nil {
		return err
	}

	for i := range cr.ReplicaAssignments {
		replicaAssignment := cr.ReplicaAssignments[i]
		pe.putInt32(replicaAssignment.PartitionID)

		if err := pe.putArrayLength(len(replicaAssignment.Replicas)); err != nil {
			return err
		}

		for j := range replicaAssignment.Replicas {
			pe.putInt32(replicaAssignment.Replicas[j])
		}
	}

	if err := pe.putArrayLength(len(cr.Configs)); err != nil {
		return err
	}

	for i := range cr.Configs {
		if err := pe.putString(cr.Configs[i].Key); err != nil {
			return err
		}

		if err := pe.putString(cr.Configs[i].Value); err != nil {
			return err
		}
	}

	return nil
}

func (cr *CreateTopicRequest) decode(pd packetDecoder, version int16) error {
	topic, err := pd.getString()
	if err != nil {
		return err
	}
	cr.Topic = topic

	numPartitions, err := pd.getInt32()
	if err != nil {
		return err
	}
	cr.NumPartitions = numPartitions

	replicationFactor, err := pd.getInt16()
	if err != nil {
		return err
	}
	cr.ReplicationFactor = replicationFactor

	partitionCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	if partitionCount > 0 {
		cr.ReplicaAssignments = make([]ReplicaAssignment, partitionCount)

		for i := range cr.ReplicaAssignments {
			replicaAssignment := ReplicaAssignment{}

			partitionID, err := pd.getInt32()
			if err != nil {
				return err
			}
			replicaAssignment.PartitionID = partitionID

			replicaCount, err := pd.getArrayLength()
			if err != nil {
				return err
			}

			replicaAssignment.Replicas = make([]int32, replicaCount)

			for j := range replicaAssignment.Replicas {
				replica, err := pd.getInt32()
				if err != nil {
					return err
				}
				replicaAssignment.Replicas[j] = replica
			}
			cr.ReplicaAssignments[i] = replicaAssignment
		}
	}

	configCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	if configCount > 0 {
		cr.Configs = make([]ConfigKV, configCount)

		for i := range cr.Configs {
			key, err := pd.getString()
			if err != nil {
				return err
			}

			value, err := pd.getString()
			if err != nil {
				return err
			}

			cr.Configs[i] = ConfigKV{Key: key, Value: value}
		}
	}

	return nil
}

func (ct *CreateTopicsRequest) encode(pe packetEncoder) error {
	if err := pe.putArrayLength(len(ct.CreateRequests)); err != nil {
		return err
	}

	for i := range ct.CreateRequests {
		if err := ct.CreateRequests[i].encode(pe); err != nil {
			return err
		}
	}

	pe.putInt32(ct.Timeout)

	return nil
}

func (ct *CreateTopicsRequest) decode(pd packetDecoder, version int16) error {
	createTopicRequestCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if createTopicRequestCount == 0 {
		return nil
	}

	ct.CreateRequests = make([]CreateTopicRequest, createTopicRequestCount)
	for i := range ct.CreateRequests {
		ct.CreateRequests[i] = CreateTopicRequest{}
		err = ct.CreateRequests[i].decode(pd, version)
		if err != nil {
			return err
		}
	}

	timeout, err := pd.getInt32()
	if err != nil {
		return err
	}

	ct.Timeout = timeout

	return nil
}

func (ct *CreateTopicsRequest) key() int16 {
	return 19
}

func (ct *CreateTopicsRequest) version() int16 {
	return 0
}

func (ct *CreateTopicsRequest) requiredVersion() KafkaVersion {
	return V0_10_0_0
}
