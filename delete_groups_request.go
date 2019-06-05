package sarama

//DeleteGroupsRequest is  used to create delete group request
type DeleteGroupsRequest struct {
	Groups []string
}

func (d *DeleteGroupsRequest) encode(pe packetEncoder) error {
	return pe.putStringArray(d.Groups)
}

func (d *DeleteGroupsRequest) decode(pd packetDecoder, version int16) (err error) {
	d.Groups, err = pd.getStringArray()
	return
}

func (d *DeleteGroupsRequest) key() int16 {
	return 42
}

func (d *DeleteGroupsRequest) version() int16 {
	return 0
}

func (d *DeleteGroupsRequest) requiredVersion() KafkaVersion {
	return V1_1_0_0
}

//AddGroup adds a given group to group requests
func (d *DeleteGroupsRequest) AddGroup(group string) {
	d.Groups = append(d.Groups, group)
}
