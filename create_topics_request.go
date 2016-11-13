package sarama

type CreateTopicRequest struct {
	Topic             string
	NumPartitions     int32
	ReplicationFactor int16
	ReplicaAssignment map[int32][]int32
	Configs           map[string]string
}

type CreateTopicsRequest struct {
	Requests []*CreateTopicRequest
	Timeout  int32
}

func (c *CreateTopicsRequest) encode(e packetEncoder) error {
	if err := e.putArrayLength(len(c.Requests)); err != nil {
		return err
	}
	for _, r := range c.Requests {
		if err := e.putString(r.Topic); err != nil {
			return err
		}
		e.putInt32(r.NumPartitions)
		e.putInt16(r.ReplicationFactor)
		if err := e.putArrayLength(len(r.ReplicaAssignment)); err != nil {
			return err
		}
		for partition, assignment := range r.ReplicaAssignment {
			e.putInt32(partition)
			for _, r := range assignment {
				e.putInt32(r)
			}
		}
		if err := e.putArrayLength(len(r.Configs)); err != nil {
			return err
		}
		for k, v := range r.Configs {
			if err := e.putString(k); err != nil {
				return err
			}
			if err := e.putString(v); err != nil {
				return err
			}
		}
	}
	e.putInt32(c.Timeout)
	return nil
}

func (c *CreateTopicsRequest) decode(d packetDecoder, version int16) error {
	var err error
	requestCount, err := d.getArrayLength()
	if err != nil {
		return err
	}
	if requestCount > 0 {
		c.Requests = make([]*CreateTopicRequest, requestCount)

		for i := 0; i < requestCount; i++ {
			c.Requests[i] = new(CreateTopicRequest)
			c.Requests[i].Topic, err = d.getString()
			if err != nil {
				return err
			}
			c.Requests[i].NumPartitions, err = d.getInt32()
			if err != nil {
				return err
			}
			c.Requests[i].ReplicationFactor, err = d.getInt16()
			if err != nil {
				return err
			}
			replicaAssignmentCount, err := d.getArrayLength()
			if replicaAssignmentCount > 0 {
				c.Requests[i].ReplicaAssignment = make(map[int32][]int32, replicaAssignmentCount)
				for j := 0; j < replicaAssignmentCount; j++ {
					partition, err := d.getInt32()
					if err != nil {
						return err
					}
					replicaCount, err := d.getArrayLength()
					if err != nil {
						return err
					}
					replicas := make([]int32, replicaCount)
					for i := range replicas {
						replicas[i], err = d.getInt32()
						if err != nil {
							return err
						}
					}
					c.Requests[i].ReplicaAssignment[partition] = replicas
				}
			}
			configCount, err := d.getArrayLength()
			if err != nil {
				return err
			}
			if configCount > 0 {
				c.Requests[i].Configs = make(map[string]string, configCount)
				for j := 0; j < configCount; j++ {
					k, err := d.getString()
					if err != nil {
						return err
					}
					v, err := d.getString()
					if err != nil {
						return err
					}
					c.Requests[i].Configs[k] = v
				}
			}
		}
	}
	c.Timeout, err = d.getInt32()
	return err
}

func (c *CreateTopicsRequest) key() int16 {
	return 19
}

func (c *CreateTopicsRequest) version() int16 {
	return 0
}

func (c *CreateTopicsRequest) requiredVersion() KafkaVersion {
	return minVersion
}
