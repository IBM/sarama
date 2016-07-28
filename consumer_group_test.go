package sarama

import (
	"strconv"
	"testing"
	"time"
)

func consume(g ConsumerGroup, name string) {
	for {
		msg := <-g.Messages()
		if msg != nil {
			g.MarkOffset(msg, "")
			// fmt.Printf("%s got partition=%d offset=%d. msg=%v\n", name, msg.Partition, msg.Offset, string(msg.Value))
		}
	}
}

func TestConsumerGroupX(t *testing.T) {

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

	// client2, err := NewClient([]string{"0.0.0.0:9092"}, conf)

	cg, err := NewConsumerGroupFromClient(client, "test", []string{"test2"})
	if err != nil {
		panic(err)
	}

	// cg2, err := NewConsumerGroupFromClient(client2, "test", []string{"test2"})

	go consume(cg, "G1")
	// go consume(cg2, "G2")
	// time.Sleep(5000 * time.Millisecond)

	// time.Sleep(100 * time.Millisecond)
	// fmt.Println("killing G2")
	// cg2.Close()
	// time.Sleep(5000 * time.Millisecond)
	// cg3, _ := NewConsumerGroupFromClient(client2, "test", []string{"test2"})
	// go consume(cg3, "G3")
	time.Sleep(10000 * time.Millisecond)
	// cg3.Close()
	err = cg.Close()
	if err != nil {
		panic(err)
	}

	client.Close()
}
