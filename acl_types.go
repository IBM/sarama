package sarama

type (
	//ACLOperation is an ACL opration value
	ACLOperation int
	//ACLPermissionType is an ACL permission type
	ACLPermissionType int
	//ACLResourceType is an ACL resource type
	ACLResourceType int
	//ACLResourcePatternType is an ACL resource pattern type
	ACLResourcePatternType int
)

// ref: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/acl/AclOperation.java
const (
	ACLOperationUnknown ACLOperation = iota
	ACLOperationAny
	ACLOperationAll
	ACLOperationRead
	ACLOperationWrite
	ACLOperationCreate
	ACLOperationDelete
	ACLOperationAlter
	ACLOperationDescribe
	ACLOperationClusterAction
	ACLOperationDescribeConfigs
	ACLOperationAlterConfigs
	ACLOperationIdempotentWrite
)

// ref: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/acl/AclPermissionType.java
const (
	ACLPermissionUnknown ACLPermissionType = iota
	ACLPermissionAny
	ACLPermissionDeny
	ACLPermissionAllow
)

// ref: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/resource/ResourceType.java
const (
	ACLResourceUnknown ACLResourceType = iota
	ACLResourceAny
	ACLResourceTopic
	ACLResourceGroup
	ACLResourceCluster
	ACLResourceTransactionalID
)

// ref: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/resource/PatternType.java
const (
	ACLPatternUnknown ACLResourcePatternType = iota
	ACLPatternAny
	ACLPatternMatch
	ACLPatternLiteral
	ACLPatternPrefixed
)
