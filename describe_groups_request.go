package sarama

//DescribeGroupsRequest is used to describe groups request
type DescribeGroupsRequest struct {
	Groups []string
}

func (d *DescribeGroupsRequest) encode(pe packetEncoder) error {
	return pe.putStringArray(d.Groups)
}

func (d *DescribeGroupsRequest) decode(pd packetDecoder, version int16) (err error) {
	d.Groups, err = pd.getStringArray()
	return
}

func (d *DescribeGroupsRequest) key() int16 {
	return 15
}

func (d *DescribeGroupsRequest) version() int16 {
	return 0
}

func (d *DescribeGroupsRequest) requiredVersion() KafkaVersion {
	return V0_9_0_0
}

//AddGroup adds a given group
func (d *DescribeGroupsRequest) AddGroup(group string) {
	d.Groups = append(d.Groups, group)
}
