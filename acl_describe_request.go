package sarama

//DescribeAclsRequest is a secribe acl request type
type DescribeAclsRequest struct {
	Version int
	ACLFilter
}

func (d *DescribeAclsRequest) encode(pe packetEncoder) error {
	d.ACLFilter.Version = d.Version
	return d.ACLFilter.encode(pe)
}

func (d *DescribeAclsRequest) decode(pd packetDecoder, version int16) (err error) {
	d.Version = int(version)
	d.ACLFilter.Version = int(version)
	return d.ACLFilter.decode(pd, version)
}

func (d *DescribeAclsRequest) key() int16 {
	return 29
}

func (d *DescribeAclsRequest) version() int16 {
	return int16(d.Version)
}

func (d *DescribeAclsRequest) requiredVersion() KafkaVersion {
	switch d.Version {
	case 1:
		return V2_0_0_0
	default:
		return V0_11_0_0
	}
}
