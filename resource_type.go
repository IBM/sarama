package sarama

type ResourceType int8

const (
	UnknownResource ResourceType = 0
	AnyResource     ResourceType = 1
	TopicResource   ResourceType = 2
	GroupResource   ResourceType = 3
	ClusterResource ResourceType = 4
	BrokerResource  ResourceType = 5
)
