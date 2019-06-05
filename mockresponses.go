package sarama

import (
	"fmt"
	"strings"
)

// TestReporter has methods matching go's testing.T to avoid importing
// `testing` in the main part of the library.
type TestReporter interface {
	Error(...interface{})
	Errorf(string, ...interface{})
	Fatal(...interface{})
	Fatalf(string, ...interface{})
}

// MockResponse is a response builder interface it defines one method that
// allows generating a response based on a request body. MockResponses are used
// to program behavior of MockBroker in tests.
type MockResponse interface {
	For(reqBody versionedDecoder) (res MockEncoder)
}

// MockWrapper is a mock response builder that returns a particular concrete
// response regardless of the actual request passed to the `For` method.
type MockWrapper struct {
	res MockEncoder
}

//MockEncoder wraps encoder type
type MockEncoder interface {
	encoder
}

//For ...
func (mw *MockWrapper) For(reqBody versionedDecoder) (res MockEncoder) {
	return mw.res
}

//NewMockWrapper ...
func NewMockWrapper(res encoder) *MockWrapper {
	return &MockWrapper{res: res}
}

// MockSequence is a mock response builder that is created from a sequence of
// concrete responses. Every time when a `MockBroker` calls its `For` method
// the next response from the sequence is returned. When the end of the
// sequence is reached the last element from the sequence is returned.
type MockSequence struct {
	responses []MockResponse
}

//NewMockSequence ...
func NewMockSequence(responses ...interface{}) *MockSequence {
	ms := &MockSequence{}
	ms.responses = make([]MockResponse, len(responses))
	for i, res := range responses {
		switch res := res.(type) {
		case MockResponse:
			ms.responses[i] = res
		case encoder:
			ms.responses[i] = NewMockWrapper(res)
		default:
			panic(fmt.Sprintf("Unexpected response type: %T", res))
		}
	}
	return ms
}

//For ...
func (mc *MockSequence) For(reqBody versionedDecoder) (res MockEncoder) {
	res = mc.responses[0].For(reqBody)
	if len(mc.responses) > 1 {
		mc.responses = mc.responses[1:]
	}
	return res
}

//MockListGroupsResponse is mock list group response
type MockListGroupsResponse struct {
	groups map[string]string
	t      TestReporter
}

//NewMockListGroupsResponse returns mock list group response type
func NewMockListGroupsResponse(t TestReporter) *MockListGroupsResponse {
	return &MockListGroupsResponse{
		groups: make(map[string]string),
		t:      t,
	}
}

//For ...
func (m *MockListGroupsResponse) For(reqBody versionedDecoder) MockEncoder {
	request := reqBody.(*ListGroupsRequest)
	_ = request
	response := &ListGroupsResponse{
		Groups: m.groups,
	}
	return response
}

//AddGroup ...
func (m *MockListGroupsResponse) AddGroup(groupID, protocolType string) *MockListGroupsResponse {
	m.groups[groupID] = protocolType
	return m
}

//MockDescribeGroupsResponse ...
type MockDescribeGroupsResponse struct {
	groups map[string]*GroupDescription
	t      TestReporter
}

//NewMockDescribeGroupsResponse ...
func NewMockDescribeGroupsResponse(t TestReporter) *MockDescribeGroupsResponse {
	return &MockDescribeGroupsResponse{
		t:      t,
		groups: make(map[string]*GroupDescription),
	}
}

//AddGroupDescription ...
func (m *MockDescribeGroupsResponse) AddGroupDescription(groupID string, description *GroupDescription) *MockDescribeGroupsResponse {
	m.groups[groupID] = description
	return m
}

//For ...
func (m *MockDescribeGroupsResponse) For(reqBody versionedDecoder) MockEncoder {
	request := reqBody.(*DescribeGroupsRequest)

	response := &DescribeGroupsResponse{}
	for _, requestedGroup := range request.Groups {
		if group, ok := m.groups[requestedGroup]; ok {
			response.Groups = append(response.Groups, group)
		} else {
			// Mimic real kafka - if a group doesn't exist, return
			// an entry with state "Dead"
			response.Groups = append(response.Groups, &GroupDescription{
				GroupID: requestedGroup,
				State:   "Dead",
			})
		}
	}

	return response
}

// MockMetadataResponse is a `MetadataResponse` builder.
type MockMetadataResponse struct {
	controllerID int32
	leaders      map[string]map[int32]int32
	brokers      map[string]int32
	t            TestReporter
}

//NewMockMetadataResponse ...
func NewMockMetadataResponse(t TestReporter) *MockMetadataResponse {
	return &MockMetadataResponse{
		leaders: make(map[string]map[int32]int32),
		brokers: make(map[string]int32),
		t:       t,
	}
}

//SetLeader ...
func (mmr *MockMetadataResponse) SetLeader(topic string, partition, brokerID int32) *MockMetadataResponse {
	partitions := mmr.leaders[topic]
	if partitions == nil {
		partitions = make(map[int32]int32)
		mmr.leaders[topic] = partitions
	}
	partitions[partition] = brokerID
	return mmr
}

//SetBroker ...
func (mmr *MockMetadataResponse) SetBroker(addr string, brokerID int32) *MockMetadataResponse {
	mmr.brokers[addr] = brokerID
	return mmr
}

//SetController ...
func (mmr *MockMetadataResponse) SetController(brokerID int32) *MockMetadataResponse {
	mmr.controllerID = brokerID
	return mmr
}

//For ...
func (mmr *MockMetadataResponse) For(reqBody versionedDecoder) MockEncoder {
	metadataRequest := reqBody.(*MetadataRequest)
	metadataResponse := &MetadataResponse{
		Version:      metadataRequest.version(),
		ControllerID: mmr.controllerID,
	}
	for addr, brokerID := range mmr.brokers {
		metadataResponse.AddBroker(addr, brokerID)
	}

	// Generate set of replicas
	replicas := []int32{}
	offlineReplicas := []int32{}
	for _, brokerID := range mmr.brokers {
		replicas = append(replicas, brokerID)
	}

	if len(metadataRequest.Topics) == 0 {
		for topic, partitions := range mmr.leaders {
			for partition, brokerID := range partitions {
				metadataResponse.AddTopicPartition(topic, partition, brokerID, replicas, replicas, offlineReplicas, ErrNoError)
			}
		}
		return metadataResponse
	}
	for _, topic := range metadataRequest.Topics {
		for partition, brokerID := range mmr.leaders[topic] {
			metadataResponse.AddTopicPartition(topic, partition, brokerID, replicas, replicas, offlineReplicas, ErrNoError)
		}
	}
	return metadataResponse
}

// MockOffsetResponse is an `OffsetResponse` builder.
type MockOffsetResponse struct {
	offsets map[string]map[int32]map[int64]int64
	t       TestReporter
	version int16
}

//NewMockOffsetResponse ...
func NewMockOffsetResponse(t TestReporter) *MockOffsetResponse {
	return &MockOffsetResponse{
		offsets: make(map[string]map[int32]map[int64]int64),
		t:       t,
	}
}

//SetVersion ...
func (mor *MockOffsetResponse) SetVersion(version int16) *MockOffsetResponse {
	mor.version = version
	return mor
}

//SetOffset ...
func (mor *MockOffsetResponse) SetOffset(topic string, partition int32, time, offset int64) *MockOffsetResponse {
	partitions := mor.offsets[topic]
	if partitions == nil {
		partitions = make(map[int32]map[int64]int64)
		mor.offsets[topic] = partitions
	}
	times := partitions[partition]
	if times == nil {
		times = make(map[int64]int64)
		partitions[partition] = times
	}
	times[time] = offset
	return mor
}

//For ...
func (mor *MockOffsetResponse) For(reqBody versionedDecoder) MockEncoder {
	offsetRequest := reqBody.(*OffsetRequest)
	offsetResponse := &OffsetResponse{Version: mor.version}
	for topic, partitions := range offsetRequest.blocks {
		for partition, block := range partitions {
			offset := mor.getOffset(topic, partition, block.time)
			offsetResponse.AddTopicPartition(topic, partition, offset)
		}
	}
	return offsetResponse
}

func (mor *MockOffsetResponse) getOffset(topic string, partition int32, time int64) int64 {
	partitions := mor.offsets[topic]
	if partitions == nil {
		mor.t.Errorf("missing topic: %s", topic)
	}
	times := partitions[partition]
	if times == nil {
		mor.t.Errorf("missing partition: %d", partition)
	}
	offset, ok := times[time]
	if !ok {
		mor.t.Errorf("missing time: %d", time)
	}
	return offset
}

// MockFetchResponse is a `FetchResponse` builder.
type MockFetchResponse struct {
	messages       map[string]map[int32]map[int64]Encoder
	highWaterMarks map[string]map[int32]int64
	t              TestReporter
	batchSize      int
	version        int16
}

//NewMockFetchResponse ...
func NewMockFetchResponse(t TestReporter, batchSize int) *MockFetchResponse {
	return &MockFetchResponse{
		messages:       make(map[string]map[int32]map[int64]Encoder),
		highWaterMarks: make(map[string]map[int32]int64),
		t:              t,
		batchSize:      batchSize,
	}
}

//SetVersion ...
func (mfr *MockFetchResponse) SetVersion(version int16) *MockFetchResponse {
	mfr.version = version
	return mfr
}

//SetMessage ...
func (mfr *MockFetchResponse) SetMessage(topic string, partition int32, offset int64, msg Encoder) *MockFetchResponse {
	partitions := mfr.messages[topic]
	if partitions == nil {
		partitions = make(map[int32]map[int64]Encoder)
		mfr.messages[topic] = partitions
	}
	messages := partitions[partition]
	if messages == nil {
		messages = make(map[int64]Encoder)
		partitions[partition] = messages
	}
	messages[offset] = msg
	return mfr
}

//SetHighWaterMark ...
func (mfr *MockFetchResponse) SetHighWaterMark(topic string, partition int32, offset int64) *MockFetchResponse {
	partitions := mfr.highWaterMarks[topic]
	if partitions == nil {
		partitions = make(map[int32]int64)
		mfr.highWaterMarks[topic] = partitions
	}
	partitions[partition] = offset
	return mfr
}

//For ...
func (mfr *MockFetchResponse) For(reqBody versionedDecoder) MockEncoder {
	fetchRequest := reqBody.(*FetchRequest)
	res := &FetchResponse{
		Version: mfr.version,
	}
	for topic, partitions := range fetchRequest.blocks {
		for partition, block := range partitions {
			initialOffset := block.fetchOffset
			offset := initialOffset
			maxOffset := initialOffset + int64(mfr.getMessageCount(topic, partition))
			for i := 0; i < mfr.batchSize && offset < maxOffset; {
				msg := mfr.getMessage(topic, partition, offset)
				if msg != nil {
					res.AddMessage(topic, partition, nil, msg, offset)
					i++
				}
				offset++
			}
			fb := res.GetBlock(topic, partition)
			if fb == nil {
				res.AddError(topic, partition, ErrNoError)
				fb = res.GetBlock(topic, partition)
			}
			fb.HighWaterMarkOffset = mfr.getHighWaterMark(topic, partition)
		}
	}
	return res
}

func (mfr *MockFetchResponse) getMessage(topic string, partition int32, offset int64) Encoder {
	partitions := mfr.messages[topic]
	if partitions == nil {
		return nil
	}
	messages := partitions[partition]
	if messages == nil {
		return nil
	}
	return messages[offset]
}

func (mfr *MockFetchResponse) getMessageCount(topic string, partition int32) int {
	partitions := mfr.messages[topic]
	if partitions == nil {
		return 0
	}
	messages := partitions[partition]
	if messages == nil {
		return 0
	}
	return len(messages)
}

func (mfr *MockFetchResponse) getHighWaterMark(topic string, partition int32) int64 {
	partitions := mfr.highWaterMarks[topic]
	if partitions == nil {
		return 0
	}
	return partitions[partition]
}

// MockConsumerMetadataResponse is a `ConsumerMetadataResponse` builder.
type MockConsumerMetadataResponse struct {
	coordinators map[string]interface{}
	t            TestReporter
}

//NewMockConsumerMetadataResponse ...
func NewMockConsumerMetadataResponse(t TestReporter) *MockConsumerMetadataResponse {
	return &MockConsumerMetadataResponse{
		coordinators: make(map[string]interface{}),
		t:            t,
	}
}

//SetCoordinator ...
func (mr *MockConsumerMetadataResponse) SetCoordinator(group string, broker *MockBroker) *MockConsumerMetadataResponse {
	mr.coordinators[group] = broker
	return mr
}

//SetError ...
func (mr *MockConsumerMetadataResponse) SetError(group string, kerror KError) *MockConsumerMetadataResponse {
	mr.coordinators[group] = kerror
	return mr
}

//For ...
func (mr *MockConsumerMetadataResponse) For(reqBody versionedDecoder) MockEncoder {
	req := reqBody.(*ConsumerMetadataRequest)
	group := req.ConsumerGroup
	res := &ConsumerMetadataResponse{}
	v := mr.coordinators[group]
	switch v := v.(type) {
	case *MockBroker:
		res.Coordinator = &Broker{id: v.BrokerID(), addr: v.Addr()}
	case KError:
		res.Err = v
	}
	return res
}

// MockFindCoordinatorResponse is a `FindCoordinatorResponse` builder.
type MockFindCoordinatorResponse struct {
	groupCoordinators map[string]interface{}
	transCoordinators map[string]interface{}
	t                 TestReporter
}

//NewMockFindCoordinatorResponse ...
func NewMockFindCoordinatorResponse(t TestReporter) *MockFindCoordinatorResponse {
	return &MockFindCoordinatorResponse{
		groupCoordinators: make(map[string]interface{}),
		transCoordinators: make(map[string]interface{}),
		t:                 t,
	}
}

//SetCoordinator ...
func (mr *MockFindCoordinatorResponse) SetCoordinator(coordinatorType CoordinatorType, group string, broker *MockBroker) *MockFindCoordinatorResponse {
	switch coordinatorType {
	case CoordinatorGroup:
		mr.groupCoordinators[group] = broker
	case CoordinatorTransaction:
		mr.transCoordinators[group] = broker
	}
	return mr
}

//SetError ...
func (mr *MockFindCoordinatorResponse) SetError(coordinatorType CoordinatorType, group string, kerror KError) *MockFindCoordinatorResponse {
	switch coordinatorType {
	case CoordinatorGroup:
		mr.groupCoordinators[group] = kerror
	case CoordinatorTransaction:
		mr.transCoordinators[group] = kerror
	}
	return mr
}

//For ...
func (mr *MockFindCoordinatorResponse) For(reqBody versionedDecoder) MockEncoder {
	req := reqBody.(*FindCoordinatorRequest)
	res := &FindCoordinatorResponse{}
	var v interface{}
	switch req.CoordinatorType {
	case CoordinatorGroup:
		v = mr.groupCoordinators[req.CoordinatorKey]
	case CoordinatorTransaction:
		v = mr.transCoordinators[req.CoordinatorKey]
	}
	switch v := v.(type) {
	case *MockBroker:
		res.Coordinator = &Broker{id: v.BrokerID(), addr: v.Addr()}
	case KError:
		res.Err = v
	}
	return res
}

// MockOffsetCommitResponse is a `OffsetCommitResponse` builder.
type MockOffsetCommitResponse struct {
	errors map[string]map[string]map[int32]KError
	t      TestReporter
}

//NewMockOffsetCommitResponse ...
func NewMockOffsetCommitResponse(t TestReporter) *MockOffsetCommitResponse {
	return &MockOffsetCommitResponse{t: t}
}

//SetError ...
func (mr *MockOffsetCommitResponse) SetError(group, topic string, partition int32, kerror KError) *MockOffsetCommitResponse {
	if mr.errors == nil {
		mr.errors = make(map[string]map[string]map[int32]KError)
	}
	topics := mr.errors[group]
	if topics == nil {
		topics = make(map[string]map[int32]KError)
		mr.errors[group] = topics
	}
	partitions := topics[topic]
	if partitions == nil {
		partitions = make(map[int32]KError)
		topics[topic] = partitions
	}
	partitions[partition] = kerror
	return mr
}

//For ...
func (mr *MockOffsetCommitResponse) For(reqBody versionedDecoder) MockEncoder {
	req := reqBody.(*OffsetCommitRequest)
	group := req.ConsumerGroup
	res := &OffsetCommitResponse{}
	for topic, partitions := range req.blocks {
		for partition := range partitions {
			res.AddError(topic, partition, mr.getError(group, topic, partition))
		}
	}
	return res
}

func (mr *MockOffsetCommitResponse) getError(group, topic string, partition int32) KError {
	topics := mr.errors[group]
	if topics == nil {
		return ErrNoError
	}
	partitions := topics[topic]
	if partitions == nil {
		return ErrNoError
	}
	kerror, ok := partitions[partition]
	if !ok {
		return ErrNoError
	}
	return kerror
}

// MockProduceResponse is a `ProduceResponse` builder.
type MockProduceResponse struct {
	version int16
	errors  map[string]map[int32]KError
	t       TestReporter
}

//NewMockProduceResponse ...
func NewMockProduceResponse(t TestReporter) *MockProduceResponse {
	return &MockProduceResponse{t: t}
}

//SetVersion ...
func (mr *MockProduceResponse) SetVersion(version int16) *MockProduceResponse {
	mr.version = version
	return mr
}

//SetError ...
func (mr *MockProduceResponse) SetError(topic string, partition int32, kerror KError) *MockProduceResponse {
	if mr.errors == nil {
		mr.errors = make(map[string]map[int32]KError)
	}
	partitions := mr.errors[topic]
	if partitions == nil {
		partitions = make(map[int32]KError)
		mr.errors[topic] = partitions
	}
	partitions[partition] = kerror
	return mr
}

//For ...
func (mr *MockProduceResponse) For(reqBody versionedDecoder) MockEncoder {
	req := reqBody.(*ProduceRequest)
	res := &ProduceResponse{
		Version: mr.version,
	}
	for topic, partitions := range req.records {
		for partition := range partitions {
			res.AddTopicPartition(topic, partition, mr.getError(topic, partition))
		}
	}
	return res
}

func (mr *MockProduceResponse) getError(topic string, partition int32) KError {
	partitions := mr.errors[topic]
	if partitions == nil {
		return ErrNoError
	}
	kerror, ok := partitions[partition]
	if !ok {
		return ErrNoError
	}
	return kerror
}

// MockOffsetFetchResponse is a `OffsetFetchResponse` builder.
type MockOffsetFetchResponse struct {
	offsets map[string]map[string]map[int32]*OffsetFetchResponseBlock
	t       TestReporter
}

//NewMockOffsetFetchResponse ...
func NewMockOffsetFetchResponse(t TestReporter) *MockOffsetFetchResponse {
	return &MockOffsetFetchResponse{t: t}
}

//SetOffset ...
func (mr *MockOffsetFetchResponse) SetOffset(group, topic string, partition int32, offset int64, metadata string, kerror KError) *MockOffsetFetchResponse {
	if mr.offsets == nil {
		mr.offsets = make(map[string]map[string]map[int32]*OffsetFetchResponseBlock)
	}
	topics := mr.offsets[group]
	if topics == nil {
		topics = make(map[string]map[int32]*OffsetFetchResponseBlock)
		mr.offsets[group] = topics
	}
	partitions := topics[topic]
	if partitions == nil {
		partitions = make(map[int32]*OffsetFetchResponseBlock)
		topics[topic] = partitions
	}
	partitions[partition] = &OffsetFetchResponseBlock{offset, 0, metadata, kerror}
	return mr
}

//For ...
func (mr *MockOffsetFetchResponse) For(reqBody versionedDecoder) MockEncoder {
	req := reqBody.(*OffsetFetchRequest)
	group := req.ConsumerGroup
	res := &OffsetFetchResponse{}
	for topic, partitions := range mr.offsets[group] {
		for partition, block := range partitions {
			res.AddBlock(topic, partition, block)
		}
	}
	return res
}

//MockCreateTopicsResponse ...
type MockCreateTopicsResponse struct {
	t TestReporter
}

//NewMockCreateTopicsResponse ...
func NewMockCreateTopicsResponse(t TestReporter) *MockCreateTopicsResponse {
	return &MockCreateTopicsResponse{t: t}
}

//For ...
func (mr *MockCreateTopicsResponse) For(reqBody versionedDecoder) MockEncoder {
	req := reqBody.(*CreateTopicsRequest)
	res := &CreateTopicsResponse{
		Version: req.Version,
	}
	res.TopicErrors = make(map[string]*TopicError)

	for topic := range req.TopicDetails {
		if res.Version >= 1 && strings.HasPrefix(topic, "_") {
			msg := "insufficient permissions to create topic with reserved prefix"
			res.TopicErrors[topic] = &TopicError{
				Err:    ErrTopicAuthorizationFailed,
				ErrMsg: &msg,
			}
			continue
		}
		res.TopicErrors[topic] = &TopicError{Err: ErrNoError}
	}
	return res
}

//MockDeleteTopicsResponse ...
type MockDeleteTopicsResponse struct {
	t TestReporter
}

//NewMockDeleteTopicsResponse ...
func NewMockDeleteTopicsResponse(t TestReporter) *MockDeleteTopicsResponse {
	return &MockDeleteTopicsResponse{t: t}
}

//For ...
func (mr *MockDeleteTopicsResponse) For(reqBody versionedDecoder) MockEncoder {
	req := reqBody.(*DeleteTopicsRequest)
	res := &DeleteTopicsResponse{}
	res.TopicErrorCodes = make(map[string]KError)

	for _, topic := range req.Topics {
		res.TopicErrorCodes[topic] = ErrNoError
	}
	return res
}

//MockCreatePartitionsResponse ...
type MockCreatePartitionsResponse struct {
	t TestReporter
}

//NewMockCreatePartitionsResponse ...
func NewMockCreatePartitionsResponse(t TestReporter) *MockCreatePartitionsResponse {
	return &MockCreatePartitionsResponse{t: t}
}

//For ...
func (mr *MockCreatePartitionsResponse) For(reqBody versionedDecoder) MockEncoder {
	req := reqBody.(*CreatePartitionsRequest)
	res := &CreatePartitionsResponse{}
	res.TopicPartitionErrors = make(map[string]*TopicPartitionError)

	for topic := range req.TopicPartitions {
		if strings.HasPrefix(topic, "_") {
			msg := "insufficient permissions to create partition on topic with reserved prefix"
			res.TopicPartitionErrors[topic] = &TopicPartitionError{
				Err:    ErrTopicAuthorizationFailed,
				ErrMsg: &msg,
			}
			continue
		}
		res.TopicPartitionErrors[topic] = &TopicPartitionError{Err: ErrNoError}
	}
	return res
}

//MockDeleteRecordsResponse ...
type MockDeleteRecordsResponse struct {
	t TestReporter
}

//NewMockDeleteRecordsResponse ...
func NewMockDeleteRecordsResponse(t TestReporter) *MockDeleteRecordsResponse {
	return &MockDeleteRecordsResponse{t: t}
}

//For ...
func (mr *MockDeleteRecordsResponse) For(reqBody versionedDecoder) MockEncoder {
	req := reqBody.(*DeleteRecordsRequest)
	res := &DeleteRecordsResponse{}
	res.Topics = make(map[string]*DeleteRecordsResponseTopic)

	for topic, deleteRecordRequestTopic := range req.Topics {
		partitions := make(map[int32]*DeleteRecordsResponsePartition)
		for partition := range deleteRecordRequestTopic.PartitionOffsets {
			partitions[partition] = &DeleteRecordsResponsePartition{Err: ErrNoError}
		}
		res.Topics[topic] = &DeleteRecordsResponseTopic{Partitions: partitions}
	}
	return res
}

//MockDescribeConfigsResponse ...
type MockDescribeConfigsResponse struct {
	t TestReporter
}

//NewMockDescribeConfigsResponse ...
func NewMockDescribeConfigsResponse(t TestReporter) *MockDescribeConfigsResponse {
	return &MockDescribeConfigsResponse{t: t}
}

//For ...
func (mr *MockDescribeConfigsResponse) For(reqBody versionedDecoder) MockEncoder {
	req := reqBody.(*DescribeConfigsRequest)
	res := &DescribeConfigsResponse{}

	for _, r := range req.Resources {
		var configEntries []*ConfigEntry
		switch r.Type {
		case TopicResource:
			configEntries = append(configEntries,
				&ConfigEntry{Name: "max.message.bytes",
					Value:     "1000000",
					ReadOnly:  false,
					Default:   true,
					Sensitive: false,
				}, &ConfigEntry{Name: "retention.ms",
					Value:     "5000",
					ReadOnly:  false,
					Default:   false,
					Sensitive: false,
				}, &ConfigEntry{Name: "password",
					Value:     "12345",
					ReadOnly:  false,
					Default:   false,
					Sensitive: true,
				})
			res.Resources = append(res.Resources, &ResourceResponse{
				Name:    r.Name,
				Configs: configEntries,
			})
		}
	}
	return res
}

//MockAlterConfigsResponse ...
type MockAlterConfigsResponse struct {
	t TestReporter
}

//NewMockAlterConfigsResponse ...
func NewMockAlterConfigsResponse(t TestReporter) *MockAlterConfigsResponse {
	return &MockAlterConfigsResponse{t: t}
}

//For ...
func (mr *MockAlterConfigsResponse) For(reqBody versionedDecoder) MockEncoder {
	req := reqBody.(*AlterConfigsRequest)
	res := &AlterConfigsResponse{}

	for _, r := range req.Resources {
		res.Resources = append(res.Resources, &AlterConfigsResourceResponse{Name: r.Name,
			Type:     TopicResource,
			ErrorMsg: "",
		})
	}
	return res
}

//MockCreateAclsResponse ...
type MockCreateAclsResponse struct {
	t TestReporter
}

//NewMockCreateAclsResponse ...
func NewMockCreateAclsResponse(t TestReporter) *MockCreateAclsResponse {
	return &MockCreateAclsResponse{t: t}
}

//For ...
func (mr *MockCreateAclsResponse) For(reqBody versionedDecoder) MockEncoder {
	req := reqBody.(*CreateACLsRequest)
	res := &CreateACLsResponse{}

	for range req.ACLCreations {
		res.ACLCreationResponses = append(res.ACLCreationResponses, &ACLCreationResponse{Err: ErrNoError})
	}
	return res
}

//MockListAclsResponse ...
type MockListAclsResponse struct {
	t TestReporter
}

//NewMockListAclsResponse ...
func NewMockListAclsResponse(t TestReporter) *MockListAclsResponse {
	return &MockListAclsResponse{t: t}
}

//For ...
func (mr *MockListAclsResponse) For(reqBody versionedDecoder) MockEncoder {
	req := reqBody.(*DescribeAclsRequest)
	res := &DescribeAclsResponse{}

	res.Err = ErrNoError
	acl := &ResourceAcls{}
	acl.Resource.ResourceName = *req.ResourceName
	acl.Resource.ResourceType = req.ResourceType
	acl.Acls = append(acl.Acls, &ACL{})
	res.ResourceAcls = append(res.ResourceAcls, acl)

	return res
}

//MockSaslAuthenticateResponse ...
type MockSaslAuthenticateResponse struct {
	t             TestReporter
	kerror        KError
	saslAuthBytes []byte
}

//NewMockSaslAuthenticateResponse ...
func NewMockSaslAuthenticateResponse(t TestReporter) *MockSaslAuthenticateResponse {
	return &MockSaslAuthenticateResponse{t: t}
}

//For ...
func (msar *MockSaslAuthenticateResponse) For(reqBody versionedDecoder) MockEncoder {
	res := &SaslAuthenticateResponse{}
	res.Err = msar.kerror
	res.SaslAuthBytes = msar.saslAuthBytes
	return res
}

//SetError ...
func (msar *MockSaslAuthenticateResponse) SetError(kerror KError) *MockSaslAuthenticateResponse {
	msar.kerror = kerror
	return msar
}

//SetAuthBytes ...
func (msar *MockSaslAuthenticateResponse) SetAuthBytes(saslAuthBytes []byte) *MockSaslAuthenticateResponse {
	msar.saslAuthBytes = saslAuthBytes
	return msar
}

//MockDeleteAclsResponse ...
type MockDeleteAclsResponse struct {
	t TestReporter
}

//MockSaslHandshakeResponse ...
type MockSaslHandshakeResponse struct {
	enabledMechanisms []string
	kerror            KError
	t                 TestReporter
}

//NewMockSaslHandshakeResponse ...
func NewMockSaslHandshakeResponse(t TestReporter) *MockSaslHandshakeResponse {
	return &MockSaslHandshakeResponse{t: t}
}

//For ...
func (mshr *MockSaslHandshakeResponse) For(reqBody versionedDecoder) MockEncoder {
	res := &SaslHandshakeResponse{}
	res.Err = mshr.kerror
	res.EnabledMechanisms = mshr.enabledMechanisms
	return res
}

//SetError ...
func (mshr *MockSaslHandshakeResponse) SetError(kerror KError) *MockSaslHandshakeResponse {
	mshr.kerror = kerror
	return mshr
}

//SetEnabledMechanisms ...
func (mshr *MockSaslHandshakeResponse) SetEnabledMechanisms(enabledMechanisms []string) *MockSaslHandshakeResponse {
	mshr.enabledMechanisms = enabledMechanisms
	return mshr
}

//NewMockDeleteAclsResponse ...
func NewMockDeleteAclsResponse(t TestReporter) *MockDeleteAclsResponse {
	return &MockDeleteAclsResponse{t: t}
}

//For ...
func (mr *MockDeleteAclsResponse) For(reqBody versionedDecoder) MockEncoder {
	req := reqBody.(*DeleteAclsRequest)
	res := &DeleteAclsResponse{}

	for range req.Filters {
		response := &FilterResponse{Err: ErrNoError}
		response.MatchingAcls = append(response.MatchingAcls, &MatchingACL{Err: ErrNoError})
		res.FilterResponses = append(res.FilterResponses, response)
	}
	return res
}
