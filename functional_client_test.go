//go:build functional

package sarama

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestFuncConnectionFailure(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	proxy := proxyForBrokerID(t, 1)
	proxy.Enabled = false
	SaveProxy(t, "kafka1")

	config := NewFunctionalTestConfig()
	config.Metadata.Retry.Max = 1

	_, err := NewClient([]string{FunctionalTestEnv.KafkaBrokerAddrs[0]}, config)
	if !errors.Is(err, ErrOutOfBrokers) {
		t.Fatal("Expected returned error to be ErrOutOfBrokers, but was: ", err)
	}
}

func TestFuncAdminNetworkErrorClosesControllerConnection(t *testing.T) {
	checkKafkaVersion(t, "0.11.0.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	kafkaVersion, err := ParseKafkaVersion(FunctionalTestEnv.KafkaVersion)
	if err != nil {
		t.Fatal(err)
	}

	config := NewFunctionalTestConfig()
	config.Version = kafkaVersion
	adminClient, err := NewClusterAdmin(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, adminClient)

	ca, ok := adminClient.(*clusterAdmin)
	if !ok {
		t.Fatalf("expected *clusterAdmin, got %T", adminClient)
	}

	controller, err := ca.Controller()
	if err != nil {
		t.Fatal(err)
	}
	if controller.ID() < 0 {
		_, controllerID, err := ca.DescribeCluster()
		if err != nil {
			t.Fatal(err)
		}
		controller, err = ca.findBroker(controllerID)
		if err != nil {
			t.Fatal(err)
		}
		_ = controller.Open(config)
	}

	// Warm up the connection so the proxy toxic applies to an established TCP session.
	metadataReq := NewMetadataRequest(config.Version, nil)
	if _, err := controller.GetMetadata(metadataReq); err != nil {
		t.Fatal(err)
	}

	proxy := proxyForBrokerID(t, controller.ID())
	addResetPeerToxic(t, proxy)
	defer resetProxies(t)

	topicName := fmt.Sprintf("net-error-topic-%d", time.Now().UnixNano())
	topicDetails := map[string]*TopicDetail{
		topicName: {
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}
	createReq := NewCreateTopicsRequest(config.Version, topicDetails, config.Admin.Timeout, false)
	if _, err := controller.CreateTopics(createReq); err == nil {
		_ = adminClient.DeleteTopic(topicName)
		t.Fatal("expected create topics to fail due to injected network error")
	}

	connected, _ := controller.Connected()
	if connected {
		t.Fatalf("expected controller connection to be closed after network error")
	}
}

func TestFuncClientMetadata(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	config := NewFunctionalTestConfig()
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 10 * time.Millisecond
	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, config)
	if err != nil {
		t.Fatal(err)
	}

	if err := client.RefreshMetadata("unknown_topic"); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Error("Expected ErrUnknownTopicOrPartition, got", err)
	}

	if _, err := client.Leader("unknown_topic", 0); !errors.Is(err, ErrUnknownTopicOrPartition) {
		t.Error("Expected ErrUnknownTopicOrPartition, got", err)
	}

	if _, err := client.Replicas("invalid/topic", 0); !errors.Is(err, ErrUnknownTopicOrPartition) && !errors.Is(err, ErrInvalidTopic) {
		t.Error("Expected ErrUnknownTopicOrPartition or ErrInvalidTopic, got", err)
	}

	partitions, err := client.Partitions("test.4")
	if err != nil {
		t.Error(err)
	}
	if len(partitions) != 4 {
		t.Errorf("Expected test.4 topic to have 4 partitions, found %v", partitions)
	}

	partitions, err = client.Partitions("test.1")
	if err != nil {
		t.Error(err)
	}
	if len(partitions) != 1 {
		t.Errorf("Expected test.1 topic to have 1 partitions, found %v", partitions)
	}

	safeClose(t, client)
}

func TestFuncClientCoordinator(t *testing.T) {
	checkKafkaVersion(t, "0.8.2")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	client, err := NewClient(FunctionalTestEnv.KafkaBrokerAddrs, NewFunctionalTestConfig())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		broker, err := client.Coordinator(fmt.Sprintf("another_new_consumer_group_%d", i))
		if err != nil {
			t.Fatal(err)
		}

		if connected, err := broker.Connected(); !connected || err != nil {
			t.Errorf("Expected to coordinator %s broker to be properly connected.", broker.Addr())
		}
	}

	safeClose(t, client)
}
