package sarama

type ConfigResourceType int8

// Taken from :
// https://cwiki.apache.org/confluence/display/KAFKA/KIP-133%3A+Describe+and+Alter+Configs+Admin+APIs#KIP-133:DescribeandAlterConfigsAdminAPIs-WireFormattypes

const (
	UnknownResource ConfigResourceType = iota
	AnyResource
	TopicResource
	GroupResource
	ClusterResource
	BrokerResource
)
