package mocks

// A "fake" client based on the post below.
// https://blog.codeship.com/creating-fakes-in-go-with-channels/

import (
    "testing"

	"github.com/Shopify/sarama"
)

// Type for all of the calls on the CAll channel.
type Call interface{}

// Needs to contain the Calls channel and t testing variable.  Any other
// specific fields needed for testing using this client need to go here as
// well.
type FakeClient struct {
    Calls chan Call
    t *testing.T

    // bool for determining if the client is closed or not.
    closed bool
}

func NewFakeClient(t *testing.T) Client {
    return FakeClient{make(chan Call), t}
}

// Create the message types here.  Each should be a struct that contains all
// of the data that is needed to handle each request.

// A default request for methods that don't accept parameters.
type defaultRequest struct {
    getMsg bool
}

type topicRequest struct {
    topic string
}

type topicPartitionRequest struct {
    topic string
    partitionID int32
}

type offsetRequest struct {
    topic string
    partitionID int32
    time int64
}

type configResponse struct {
    config *Config
}

type topicResponse struct {
    topics []string
    err error
}

type partitionResponse struct {
    partitions []int32
    err error
}

type brokerResponse struct {
    broker *Broker
    err error
}

type replicaResponse struct {
    replicas []int32
    err error
}

type errorResponse struct {
    err error
}

type offsetResponse struct {
    offset int64
    err error
}

func (client *Client) Config() *Config{
    client.Calls <- defaultMessage{true}
    return (<-client.Calls).(configResponse).config
}

func (client *Client) AssertConfig(config *Config) {
    (<-client.Calls).(defaultRequest)
    client.Calls <- configResponse{config}
}

func (client *Client) Topics() ([]string, error){
    client.Calls <- defaultMessage{true}
    resp := (<-client.Calls).(topicResponse)
    return resp.topics, resp.err
}

func (client *Client) AssertTopics(topics []string, err error){
    (<-client.Calls).(defaultRequest)
    client.Calls <- topicResponse{topics, err}
}

func (client *Client) Partitions(topic string) ([]int32, error){
    client.Calls <- topicRequest{topic}
    resp := (<-client.Calls).(partitionResponse)
    return resp.partitions, resp.err
}

func (client *Client) AssertPartitions(topic string, partitions []int32, err error){
    req := (<-client.Calls).(topicRequest)
    if req.topic != topic {
        client.t.Error("Expected topic ", topic, " but got ", req.topic)
    }
    client.Calls <- PartitionRespnose{partitions, err}
}

func (client *Client) WritablePartitions(topic string) ([]int32, error){
    client.Calls <- topicRequest{topic}
    resp := (<-client.Calls).(partitionResponse)
    return resp.partitions, resp.err
}

func (client *Client) AssertWritablePartitions(topic string, partitions []int32, err error){
    req := (<-client.Calls).(topicRequest)
    if req.topic != topic {
        client.t.Error("Expected topic ", topic, " but got ", req.topic)
    }
    client.Calls <- partitionRespnose{partitions, err}
}

func (client *Client) Leader(topic string, partitionID int32) (*Broker, error){
    client.Calls <- topicPartitionRequest{topic, partitionID}
    resp := (<-client.Calls).(brokerResponse)
    return resp.broker, resp.err
}

func (client *Client) AssertLeader(topic string, partitionID int32, broker *Broker, err error){
    req := (<-client.Calls).(topicPartitionRequest)
    if req.topic != topic && req.partitionID != partitionID {
        client.t.Error("Expected topic ", topic, " but got ", req.topic)
        client.t.Error("Expected partitionID ", partitionID, " but got ", req.partitionID)
    }
    client.Calls <- brokerRespnose{broker, err}
}

func (client *Client) Replicas(topic string, partitionID int32) ([]int32, error){
    client.Calls <- topicPartitionRequest{topic, partitionID}
    resp := (<-client.Calls).(replicaResponse)
    return resp.replicas, resp.err
}

func (client *Client) AssertReplicas(topic string, partitionID int32, replicas []int32, err error){
    req := (<-client.Calls).(topicPartitionRequest)
    if req.topic != topic && req.partitionID != partitionID {
        client.t.Error("Expected topic ", topic, " but got ", req.topic)
        client.t.Error("Expected partitionID ", partitionID, " but got ", req.partitionID)
    }
    client.Calls <- replicaRespnose{replicas, err}
}

func (client *Client) RefreshMetadata(topics ...string) error{
    client.Calls <- defaultMessage{true}
    return (<-client.Calls).(errorResponse).err
}

func (client *Client) AssertRefreshMetadata(err error) {
    (<-client.Calls).(defaultRequest)
    client.Calls <- errorResponse{error}
}

func (client *Client) GetOffset(topic string, partitionID int32, time int64) (int64, error){
    client.Calls <- offsetRequest{topic, partitionID, time}
    resp := (<-client.Calls).(replicaResponse)
    return resp.replicas, resp.err
}

func (client *Client) AssertGetOffset(topic string, partitionID int32, time int64, offset int64, err error){
    req := (<-client.Calls).(offsetRequest)
    if req.topic != topic && req.partitionID != partitionID && req.time == time{
        client.t.Error("Expected topic ", topic, " but got ", req.topic)
        client.t.Error("Expected partitionID ", partitionID, " but got ", req.partitionID)
        client.t.Error("Expected time ", time, " but got ", req.time)
    }
    client.Calls <- offsetRespnose{offset, err}
}

func (client *Client) Close() error{
    close(client.Calls)
    closed = true
}

func (client *Client) Closed() bool{
    return closed
}

func (client *Client) AssertDone() {
    if _, more := <-client.Calls; more {
        client.t.Fatal("There are unused calls left in the Calls channel.")
    }
}
