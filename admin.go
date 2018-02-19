package sarama

import "errors"

// The administrative client for Kafka, which supports managing and inspecting topics,
// brokers, configurations and ACLs. The minimum broker version required is 0.10.0.0.
// Methods with stricter requirements will specify the minimum broker version required.
// This client was introduced in 0.11.0.0 and the API is still evolving.
// You MUST call Close() on a client to avoid leaks
type ClusterAdmin interface {
	// Creates a new topic .This operation is supported by brokers with version 0.10.1.0 or higher.
	// It may take several seconds after CreateTopic returns success for all the brokers
	// to become aware that the topics have been created. During this time, ClusterAdmin#listTopics
	// and ClusterAdmin#describeTopics may not return information about the new topics.
	// This operation is supported by brokers with version 0.10.1.0 or higher.
	// The validateOnly option is supported from version 0.10.2.0.
	CreateTopic(topic string, detail *TopicDetail) error

	// Delete a batch of topics.
	// It may take several seconds after the DeleteTopic returns success
	// for all the brokers to become aware that the topics are gone.
	// During this time, ClusterAdmin#listTopics and ClusterAdmin#describeTopics
	// may continue to return information about the deleted topics.
	// If delete.topic.enable is false on the brokers, deleteTopics will mark
	// the topics for deletion, but not actually delete them.
	// This operation is supported by brokers with version 0.10.1.0 or higher.
	DeleteTopic(topic string) error

	// Increase the number of partitions of the topics  according to the corresponding values.
	// If partitions are increased for a topic that has a key, the partition logic or ordering of
	// the messages will be affected. It may take several seconds after this method returns
	// success for all the brokers to become aware that the partitions have been created.
	// During this time, ClusterAdmin#describeTopics may not return information about the
	// new partitions. This operation is supported by brokers with version 1.0.0 or higher.
	CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error

	//Delete records whose offset is smaller than the given offset of the corresponding partition.
	//This operation is supported by brokers with version 0.11.0.0 or higher.
	DeleteRecords(topic string, partitionOffsets map[int32]int64) error

	// Get the configuration for the specified resources.
	// The returned configuration includes default values and the Default is true
	// can be used to distinguish them from user supplied values.
	// Config entries where ReadOnly is true cannot be updated.
	// The value of config entries where Sensitive is true is always nil so
	// sensitive information is not disclosed.
	// This operation is supported by brokers with version 0.11.0.0 or higher.
	DescribeConfig(resource ConfigResource) ([]ConfigEntry, error)

	// Update the configuration for the specified resources with the default options.
	// This operation is supported by brokers with version 0.11.0.0 or higher.
	// The resources with their configs (topic is the only resource type with configs
	// that can be updated currently Updates are not transactional so they may succeed
	// for some resources while fail for others. The configs for
	// a particular resource are updated atomically.
	AlterConfig(resourceType ConfigResourceType, name string, entries map[string]*string, validateOnly bool) error

	// Creates access control lists (ACLs) which are bound to specific resources.
	// This operation is not transactional so it may succeed for some ACLs while fail for others.
	// If you attempt to add an ACL that duplicates an existing ACL, no error will be raised, but
	// no changes will be made.This operation is supported by brokers with version 0.11.0.0 or higher.
	CreateAcl(resource Resource, acl Acl) error

	ListAcls(filter AclFilter) ([]ResourceAcls, error)

	// Deletes access control lists (ACLs) according to the supplied filters.
	// This operation is not transactional so it may succeed for some ACLs while fail for others.
	// This operation is supported by brokers with version 0.11.0.0 or higher.
	DeleteAcl(filter AclFilter, validateOnly bool) ([]MatchingAcl, error)

	// Close shuts down the admin and closes underlying client.
	Close() error
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

	ca := &clusterAdmin{
		client: client,
		conf:   client.Config(),
	}
	return ca, nil
}

func (ca *clusterAdmin) Close() error {
	return ca.client.Close()
}

func (ca *clusterAdmin) any() *Broker {
	return ca.client.Any()
}

func (ca *clusterAdmin) CreateTopic(topic string, detail *TopicDetail) error {

	if topic == "" {
		return ErrInvalidTopic
	}

	if detail == nil {
		return ErrInvalidInput
	}

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

	b := ca.any()

	rsp, err := b.CreateTopics(request)
	if err != nil {
		return err
	}

	topicErr, ok := rsp.TopicErrors[topic]
	if !ok {
		return ErrReferenceNodeNotFound
	}

	if topicErr.Err != ErrNoError {
		return topicErr.Err
	}

	return nil
}

func (ca *clusterAdmin) DeleteTopic(topic string) error {

	if topic == "" {
		return ErrInvalidTopic
	}

	request := &DeleteTopicsRequest{Topics: []string{topic}}

	if ca.conf.Version.IsAtLeast(V0_11_0_0) {
		request.Version = 1
	}

	b := ca.any()

	rsp, err := b.DeleteTopics(request)
	if err != nil {
		return err
	}

	topicErr, ok := rsp.TopicErrorCodes[topic]
	if !ok {
		return ErrReferenceNodeNotFound
	}

	if topicErr != ErrNoError {
		return topicErr
	}
	return nil
}

func (ca *clusterAdmin) CreatePartitions(topic string, count int32, assignment [][]int32, validateOnly bool) error {
	if topic == "" {
		return ErrInvalidTopic
	}

	topicPartitions := make(map[string]*TopicPartition)
	topicPartitions[topic] = &TopicPartition{Count: count, Assignment: assignment}

	request := &CreatePartitionsRequest{
		TopicPartitions: topicPartitions,
	}

	b := ca.any()

	rsp, err := b.CreatePartitions(request)
	if err != nil {
		return err
	}

	topicErr, ok := rsp.TopicPartitionErrors[topic]
	if !ok {
		return ErrReferenceNodeNotFound
	}

	if topicErr.Err != ErrNoError {
		return topicErr.Err
	}

	return nil
}

func (ca *clusterAdmin) DeleteRecords(topic string, partitionOffsets map[int32]int64) error {

	if topic == "" {
		return ErrInvalidTopic
	}

	topics := make(map[string]*DeleteRecordsRequestTopic)
	topics[topic] = &DeleteRecordsRequestTopic{PartitionOffsets: partitionOffsets}
	request := &DeleteRecordsRequest{
		Topics: topics}

	b := ca.any()

	rsp, err := b.DeleteRecords(request)
	if err != nil {
		return err
	}

	_, ok := rsp.Topics[topic]
	if !ok {
		return ErrReferenceNodeNotFound
	}

	//todo since we are dealing with couple of partitions it would be good if we return slice of errors
	//for each partition instead of one error
	return nil
}

func (ca *clusterAdmin) DescribeConfig(resource ConfigResource) ([]ConfigEntry, error) {

	var entries []ConfigEntry
	var resources []*ConfigResource
	resources = append(resources, &resource)

	request := &DescribeConfigsRequest{
		Resources: resources,
	}

	b := ca.any()

	rsp, err := b.DescribeConfigs(request)
	if err != nil {
		return nil, err
	}

	for _, rspResource := range rsp.Resources {
		if rspResource.Name == resource.Name {
			if rspResource.ErrorMsg != "" {
				return nil, errors.New(rspResource.ErrorMsg)
			}
			for _, cfgEntry := range rspResource.Configs {
				entries = append(entries, *cfgEntry)
			}
		}
	}
	return entries, nil
}

func (ca *clusterAdmin) AlterConfig(resourceType ConfigResourceType, name string, entries map[string]*string, validateOnly bool) error {

	var resources []*AlterConfigsResource
	resources = append(resources, &AlterConfigsResource{
		Type:          resourceType,
		Name:          name,
		ConfigEntries: entries,
	})

	request := &AlterConfigsRequest{
		Resources:    resources,
		ValidateOnly: validateOnly,
	}

	b := ca.any()

	rsp, err := b.AlterConfigs(request)
	if err != nil {
		return err
	}

	for _, rspResource := range rsp.Resources {
		if rspResource.Name == name {
			if rspResource.ErrorMsg != "" {
				return errors.New(rspResource.ErrorMsg)
			}
		}
	}
	return nil
}

func (ca *clusterAdmin) CreateAcl(resource Resource, acl Acl) error {

	var acls []*AclCreation
	acls = append(acls, &AclCreation{resource, acl})
	request := &CreateAclsRequest{AclCreations: acls}

	b := ca.any()

	_, err := b.CreateAcls(request)
	if err != nil {
		return err
	}

	return nil
}

func (ca *clusterAdmin) ListAcls(filter AclFilter) ([]ResourceAcls, error) {
	return nil, nil
}

func (ca *clusterAdmin) DeleteAcl(filter AclFilter, validateOnly bool) ([]MatchingAcl, error) {
	var filters []*AclFilter
	filters = append(filters, &filter)
	request := &DeleteAclsRequest{Filters: filters}

	b := ca.any()

	rsp, err := b.DeleteAcls(request)
	if err != nil {
		return nil, err
	}

	var mAcls []MatchingAcl
	for _, fr := range rsp.FilterResponses {
		for _, mAcl := range fr.MatchingAcls {
			mAcls = append(mAcls, *mAcl)
		}

	}
	return mAcls, nil
}
