package sarama

import (
	"errors"
	"testing"
	"time"
)

func TestFuncConsumerGroupPartitioning(t *testing.T) {
	checkKafkaVersion(t, "0.10.1")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	errs := make(chan error, 2)
	go func() {
		errs <- testFuncConsumerGroupPartitioning()
	}()
	go func() {
		time.Sleep(4 * time.Second)
		errs <- testFuncConsumerGroupPartitioning()
	}()

	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}

func testFuncConsumerGroupPartitioning() error {
	config := NewConfig()
	config.Version = V0_10_1_0

	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()

	member, err := NewConsumerGroup(kafkaBrokers, "sarama.TestFuncConsumerGroupPartitioning", config)
	if err != nil {
		return err
	}
	defer member.Close()

	for {
		sess, err := member.Subscribe([]string{"test.4"})
		if err != nil {
			return err
		}

		if claims := sess.Claims()["test.4"]; len(claims) == 2 {
			break
		}

		select {
		case <-sess.Done():
			if err := sess.Release(); err != nil {
				return err
			}
		case <-deadline.C:
			return errors.New("timeout")
		}
	}
	return member.Close()
}
