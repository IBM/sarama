package sarama

//ConfigResourceType is a type for config resource
type ConfigResourceType int8

// Note that the ResourceType described here is only accurate for ACLs:
//   https://cwiki.apache.org/confluence/display/KAFKA/KIP-133%3A+Describe+and+Alter+Configs+Admin+APIs#KIP-133:DescribeandAlterConfigsAdminAPIs-WireFormattypes
// See the following for the ConfigResource type that is used for Describe and Alter Config operations.
//   https://github.com/apache/kafka/blob/2.3/clients/src/main/java/org/apache/kafka/common/config/ConfigResource.java#L36

const (
	//UnknownResource constant type
	UnknownResource ConfigResourceType = iota
	//TopicResource constant type
	TopicResource = 2
	//BrokerResource constant type
	BrokerResource = 4
)
