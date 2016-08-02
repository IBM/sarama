package sarama

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
)

func safeProduce(n int, p SyncProducer, t *testing.T) (offsets []int64) {
	offsets = make([]int64, n, n)
	for i := 0; i < n; i++ {
		pm := &ProducerMessage{Topic: "test.4", Value: StringEncoder(fmt.Sprintf("Test %d", i)), Partition: int32(i % 4)}
		_, offset, err := p.SendMessage(pm)
		if err != nil {
			t.Fatal(err)
		}
		offsets[i] = offset
	}
	return offsets
}

func safeConsumerGroup(t *testing.T) ConsumerGroup {
	conf := NewConfig()
	conf.Version = V0_10_0_0
	conf.Group.Return.Notifications = true
	conf.ClientID = "test"
	cg, err := NewConsumerGroup(kafkaBrokers, "group.1", []string{"test.4"}, conf)
	if err != nil {
		t.Fatal(err)
	}
	return cg
}

func checkErr(err error, t *testing.T) {
	if err != nil {
		t.Fatal(err)
	}
}

func TestFunctionalConsumerGroup(t *testing.T) {
	checkKafkaVersion(t, "0.10.0")
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	pConf := NewConfig()
	pConf.Producer.Partitioner = NewManualPartitioner
	p, err := NewSyncProducer(kafkaBrokers, pConf)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, p)
	safeProduce(1, p, t)
	// lets mark everything first as this group might exist already
	client, err := NewClient(kafkaBrokers, nil)
	checkErr(err, t)
	om, err := NewOffsetManagerFromClient("group.1", client)
	checkErr(err, t)
	for i := int32(0); i < 4; i++ {
		offset, err := client.GetOffset("test.4", i, OffsetNewest)
		checkErr(err, t)
		pom, err := om.ManagePartition("test.4", i)
		checkErr(err, t)
		pom.MarkOffset(offset, "")
		err = pom.Close()
		checkErr(err, t)
	}
	om.Commit()
	om.Close()
	client.Close()

	// consume 4 messages by one
	cg := safeConsumerGroup(t)
	<-cg.Notifications()
	offsets := safeProduce(4, p, t)

	// read & commit messages
	var partitions []int
	// var commitedOffsets = make([]int64, 4, 4)
	for i := 0; i < 4; i++ {
		msg := <-cg.Messages()
		cg.MarkMessage(msg, "")
		partitions = append(partitions, int(msg.Partition))

		if offsets[msg.Partition] != msg.Offset {
			t.Errorf("commited offset %d does not match produced offset %d", msg.Offset, offsets[msg.Partition])
		}
	}

	// should have one message from each partition
	sort.Ints(partitions)
	if !reflect.DeepEqual(partitions, []int{0, 1, 2, 3}) {
		t.Errorf("Messages are not properly distributed: %v", partitions)
	}

	// force a flush
	safeClose(t, cg)

	// recreate
	cg = safeConsumerGroup(t)
	<-cg.Notifications()

	for i := 0; i < 4; i++ {
		next, _ := cg.(*consumerGroup).managed[TopicPartition{"test.4", int32(i)}].pom.NextOffset()
		if next != offsets[i] {
			t.Errorf("offset was not properly commited. got %d expected %d partition=%d", next, offsets[i], i)
		}
	}

	// add a second consumer - which should claim two partitions
	cg2 := safeConsumerGroup(t)

	<-cg.Notifications()
	n := <-cg2.Notifications()
	if len(n.Claimed) != 1 || len(n.Claimed["test.4"]) != 2 {
		t.Errorf("Second consumer should have claimed two partitions but claimed %v", n.Claimed)
	}

	// now lets produce for more message .. every consumer should receive two of them
	safeProduce(4, p, t)

	var r1, r2 int
	for {
		if r1 == 2 && r2 == 2 {
			break
		}
		select {
		case <-time.After(10 * time.Second):
			t.Fatal("Each consumer should have received two messages but didn't in the last 10s")
		case <-cg.Messages():
			r1++
		case <-cg2.Messages():
			r2++
		}
	}

	// lets add two more consumers - so each consumer has one partition
	cg3 := safeConsumerGroup(t)
	cg4 := safeConsumerGroup(t)

	var cgs = []ConsumerGroup{cg, cg2, cg3, cg4}

	for i := 0; i < 4; i++ {
		n := <-cgs[i].Notifications()
		if len(n.Current["test.4"]) != 1 {
			t.Errorf("consumer group %d should have one partition but has %#v", i, n)
		}
	}
	// a fifth consumer should not be able to claim anything
	cg5 := safeConsumerGroup(t)

	cgs = append(cgs, cg5)

	var withoutPartition ConsumerGroup
	var consuming int
	for i := 0; i < 5; i++ {
		n := <-cgs[i].Notifications()
		if len(n.Current["test.4"]) == 0 {
			withoutPartition = cgs[i]
		} else {
			consuming++
		}
	}

	if withoutPartition == nil {
		t.Error("There should be at least one consumer that has no partition assigned")
	}

	if consuming != 4 {
		t.Error("There should be exactly four active consumers but we have %d.", consuming)
	}

	// close one consumer that is not the one withoutPartition
	for _, c := range cgs {
		if c != withoutPartition {
			safeClose(t, c)
			break
		}
	}

	n = <-withoutPartition.Notifications()
	if len(n.Current["test.4"]) != 1 {
		t.Errorf("Consumer group should have taken over orphaned partition")
	}

	for _, c := range cgs {
		safeClose(t, c)
	}

}
