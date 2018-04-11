package sarama

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleTopicAndBrokerUsage(t *testing.T) {
	subject := TopicAndBroker{}
	err := subject.Set("test:localhost:9092")
	assert.Nil(t, err)
	err = subject.Set("test2:localhost:9092")
	assert.Nil(t, err)

	assert.True(t, subject.Available())
	assert.False(t, subject.IsEmpty())
	assert.Equal(t, subject.FirstBrokers(), "localhost:9092")
	assert.Equal(t, subject.FirstTopic(), "test")
}
