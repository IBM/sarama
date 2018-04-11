package sarama

import (
	"fmt"
	"reflect"
	"strings"
)

// TopicAndBroker contains several topic for possible several brokers
type TopicAndBroker map[string]string

// String returns string representation
func (cb *TopicAndBroker) String() string {
	return fmt.Sprint(*cb)
}

// Type
func (cb *TopicAndBroker) Type() string {
	return reflect.TypeOf(map[string]string{}).String()
}

// Set the value
func (cb *TopicAndBroker) Set(value string) error {
	TopicAndBroker := strings.SplitN(value, ":", 2)
	if len(TopicAndBroker) < 2 {
		return fmt.Errorf("wrong input: %s", value)
	}
	(*cb)[TopicAndBroker[0]] = TopicAndBroker[1]
	return nil
}

// FirstBrokers return first broker or empty string
func (cb *TopicAndBroker) FirstBrokers() string {
	if cb.IsEmpty() {
		return ""
	}
	for _, v := range *cb {
		return v
	}
	return ""
}

// FirstTopic return first topic or empty string
func (cb *TopicAndBroker) FirstTopic() string {
	if cb.IsEmpty() {
		return ""
	}
	for k := range *cb {
		return k
	}
	return ""
}

// Available Check if there are any topic avaliable
func (cb *TopicAndBroker) Available() bool {
	return cb != nil && len(*cb) > 0
}

// IsEmpty return true if empty
func (cb *TopicAndBroker) IsEmpty() bool {
	return cb == nil || len(*cb) == 0
}
