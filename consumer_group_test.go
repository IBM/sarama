package sarama

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
)

func consume(g ConsumerGroup, name string) {
	for {
		msg := <-g.Messages()
		if msg != nil {
			g.MarkMessage(msg, "")
			// fmt.Printf("%s got partition=%d offset=%d. msg=%v\n", name, msg.Partition, msg.Offset, string(msg.Value))
		}
	}
}

func TestConsumerGroupOffsetCorrection(t *testing.T) {
	return
	conf := NewConfig()
	conf.Version = V0_10_0_0
	conf.Producer.Return.Successes = true
	// conf.Consumer.Offsets.Initial = OffsetOldest
	client, err := NewClient([]string{"0.0.0.0:9092"}, conf)
	if err != nil {
		panic(err)
	}

	offset, _ := client.GetOffset("test", 0, OffsetNewest)
	fmt.Println(offset)

	consumer, _ := NewConsumerFromClient(client)
	pc, err := consumer.ConsumePartition("test", 0, 3)
	if err != nil {
		panic(err)
	}

	m, ok := <-pc.Messages()
	if !ok {
		fmt.Println("NOT OK")
	}

	spew.Dump(m)

	// // one test producer
	// p, err := NewSyncProducerFromClient(client)
	// if err != nil {
	// 	panic(err)
	// }
	// p.SendMessage(&ProducerMessage{Topic: "test", Value: StringEncoder("test3")})
	// time.Sleep(1000 * time.Millisecond)

	// p.Close()
	client.Close()

}

func TestConsumerGroupX(t *testing.T) {
	return

	conf := NewConfig()
	conf.Version = V0_10_0_0
	// conf.Consumer.Offsets.Initial = OffsetOldest
	client, err := NewClient([]string{"0.0.0.0:9092"}, conf)
	if err != nil {
		panic(err)
	}

	// one test producer
	p, err := NewSyncProducerFromClient(client)

	if err != nil {

		panic(err)
	}

	// forever
	go func() {
		i := int(time.Now().Unix() - 1469712195)
		for {
			i++
			// part, off, err := p.SendMessage(&ProducerMessage{Topic: "test2", Value: StringEncoder(strconv.Itoa(i))})
			p.SendMessage(&ProducerMessage{Topic: "test2", Value: StringEncoder(strconv.Itoa(i))})

			// fmt.Printf("msg to partition=%d offset=%d. err=%v\n", part, off, err)
			time.Sleep(time.Millisecond * 1000)
		}
	}()

	client2, err := NewClient([]string{"0.0.0.0:9092"}, conf)
	client3, err := NewClient([]string{"0.0.0.0:9092"}, conf)
	client4, err := NewClient([]string{"0.0.0.0:9092"}, conf)
	client5, err := NewClient([]string{"0.0.0.0:9092"}, conf)

	cg, err := NewConsumerGroupFromClient(client, "test", []string{"test2"})
	if err != nil {
		panic(err)
	}

	cg2, err := NewConsumerGroupFromClient(client2, "test", []string{"test2"})
	cg3, err := NewConsumerGroupFromClient(client3, "test", []string{"test2"})
	cg4, err := NewConsumerGroupFromClient(client4, "test", []string{"test2"})

	go consume(cg, "G1")
	time.Sleep(1000 * time.Millisecond)

	go consume(cg2, "G2")
	time.Sleep(1000 * time.Millisecond)

	go consume(cg3, "G3")
	time.Sleep(1000 * time.Millisecond)

	go consume(cg4, "G4")

	time.Sleep(1000 * time.Millisecond)

	cg5, err := NewConsumerGroupFromClient(client5, "test", []string{"test2"})
	go consume(cg5, "G5")

	time.Sleep(1000 * time.Millisecond)

	fmt.Println("killing G2")
	cg2.Close()
	// time.Sleep(5000 * time.Millisecond)
	// cg3, _ := NewConsumerGroupFromClient(client2, "test", []string{"test2"})
	// go consume(cg3, "G3")
	// time.Sleep(5000 * time.Millisecond)
	// cg3.Close()
	time.Sleep(20000 * time.Millisecond)
	// cg3.Close()
	// err = cg.Close()
	// if err != nil {
	// 	panic(err)
	// }

	client.Close()
}
