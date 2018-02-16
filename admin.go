package sarama

type ClusterAdmin interface {
	CreateTopic(topic string, detail *TopicDetail) error
	DeleteTopic(topic string) error
	CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error
	DeleteRecords(topic string, partitionOffsets map[int32]int64) error
	DescribeConfig(resource ConfigResource) ([]ConfigEntry, error)
	AlterConfig(resourceType ConfigResourceType, name string, entries map[string]*string, validateOnly bool) error
	CreateAcl(resource Resource, acl Acl) error
	ListAcls(filter AclFilter) ([]ResourceAcls, error)
	DeleteAcl(filter AclFilter, validateOnly bool) ([]MatchingAcl, error)
}

type clusterAdmin struct {
	client Client
	conf   *Config
}

// NewClusterAdmin creates a new ClusterAdmin using the given broker addresses and configuration.
func NewClusterAdmin(addrs []string, conf *Config) (ClusterAdmin, error) {
	client, err := NewClient(addrs, conf)
	if err != nil {
		return nil, err
	}

	// Check that we are not dealing with a closed Client before processing any other arguments
	if client.Closed() {
		return nil, ErrClosedClient
	}

	ca := &clusterAdmin{
		client: client,
		conf:   client.Config(),
	}

	return ca, nil
}

func (ca *clusterAdmin) handleResponses(rsp interface{}) {
	switch rsp.(type) {
	case nil:
		break
	case CreateTopicsResponse:
		Logger.Print("topic errors", rsp.(CreateTopicsResponse).TopicErrors)
	case DeleteTopicsResponse:
		Logger.Print("topic errors", rsp.(DeleteTopicsResponse).TopicErrorCodes)
	default:
		break
	}
}

func (ca *clusterAdmin) CreateTopic(topic string, detail *TopicDetail) error {
	topicDetails := make(map[string]*TopicDetail)
	topicDetails[topic] = detail

	request := &CreateTopicsRequest{
		TopicDetails: topicDetails,
	}

	if ca.conf.Version.IsAtLeast(V0_11_0_0) {
		request.Version = 1
	}
	if ca.conf.Version.IsAtLeast(V1_0_0_0) {
		request.Version = 2
	}

	b := ca.client.Any()
	rsp, err := b.CreateTopics(request)
	if err != nil {
		return err
	}
	ca.handleResponses(rsp)
	return nil
}

func (ca *clusterAdmin) DeleteTopic(topic string) error {
	request := &DeleteTopicsRequest{Topics: []string{topic}}
	if ca.conf.Version.IsAtLeast(V0_11_0_0) {
		request.Version = 1
	}

	b := ca.client.Any()
	rsp, err := b.DeleteTopics(request)
	if err != nil {
		return err
	}
	ca.handleResponses(rsp)
	return nil
}

func (ca *clusterAdmin) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	return nil
}
func (ca *clusterAdmin) DeleteRecords(topic string, partitionOffsets map[int32]int64) error {
	topics := make(map[string]*DeleteRecordsRequestTopic)
	topics[topic] = &DeleteRecordsRequestTopic{PartitionOffsets: partitionOffsets}
	request := &DeleteRecordsRequest{Topics: topics}

	b := ca.client.Any()
	rsp, err := b.DeleteRecords(request)
	if err != nil {
		return err
	}
	ca.handleResponses(rsp)
	return nil
}

func (ca *clusterAdmin) DescribeConfig(resource ConfigResource) ([]ConfigEntry, error) {
	return nil, nil
}
func (ca *clusterAdmin) AlterConfig(resourceType ConfigResourceType, name string, entries map[string]*string, validateOnly bool) error {
	return nil
}

func (ca *clusterAdmin) CreateAcl(resource Resource, acl Acl) error {
	var acls []*AclCreation
	acls = append(acls, &AclCreation{resource, acl})
	request := &CreateAclsRequest{AclCreations: acls}

	b := ca.client.Any()
	rsp, err := b.CreateAcls(request)
	if err != nil {
		return err
	}
	ca.handleResponses(rsp)
	return nil
}
func (ca *clusterAdmin) ListAcls(filter AclFilter) ([]ResourceAcls, error) {
	return nil, nil
}
func (ca *clusterAdmin) DeleteAcl(filter AclFilter, validateOnly bool) ([]MatchingAcl, error) {
	var filters []*AclFilter
	filters = append(filters, &filter)
	request := &DeleteAclsRequest{Filters: filters}

	b := ca.client.Any()
	rsp, err := b.DeleteAcls(request)
	if err != nil {
		return nil, err
	}

	ca.handleResponses(rsp)
	var mAcls []MatchingAcl
	for _, fr := range rsp.FilterResponses {
		for _, mAcl := range fr.MatchingAcls {
			mAcls = append(mAcls, *mAcl)
		}

	}
	return mAcls, nil
}
