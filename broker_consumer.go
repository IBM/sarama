package sarama

import (
	"errors"
	"github.com/Sirupsen/tomb"

	"sync"
	"time"
)

type brokerConsumer struct {
	m        sync.Mutex
	intake   chan *consumerTP
	Broker   *Broker
	consumer consumerish
	tomb     tomb.Tomb
	config   *ConsumerConfig
	clientID string
}

type consumerish interface {
	removeBrokerConsumer(*brokerConsumer)
	sendEvent(*ConsumerEvent)
	allocateConsumerTP(tp *consumerTP)
}

func newBrokerConsumer(m consumerish, config *ConsumerConfig, broker *Broker, clientID string) *brokerConsumer {
	bc := &brokerConsumer{
		intake:   make(chan *consumerTP, 16),
		Broker:   broker,
		consumer: m,
		config:   config,
		clientID: clientID,
	}
	go withRecover(bc.run)
	return bc
}

func (bc *brokerConsumer) Close() error {
	bc.tomb.Kill(errors.New("Close() called"))
	return bc.tomb.Wait()
}

func (bc *brokerConsumer) run() {
	defer bc.tomb.Done()

	var tps []*consumerTP

	in := make(chan []*consumerTP)
	out := make(chan []*consumerTP)
	go func() {
		for {
			select {
			case tps := <-in:
				out <- bc.fetchMessages(tps)
			case <-bc.tomb.Dying():
				return
			}
		}
	}()

	for {
		select {
		case <-bc.tomb.Dying():
			goto shutdown
		case tp := <-bc.intake:
			bc.m.Lock()
			tps = append(tps, tp)
			if len(tps) == 1 {
				in <- tps
			}
			bc.m.Unlock()
		case tps := <-out:
			if len(tps) == 0 {
				goto shutdown
			}
			in <- tps
		}
	}

shutdown:
	bc.consumer.removeBrokerConsumer(bc)
	close(bc.intake)
	if bc.Broker.conn != nil {
		// calling broker.Close() doesn't interrupt pending requests. We would like
		// to force any pending requests to terminate prematurely, since we would be
		// discarding their output anyway.
		bc.Broker.conn.Close()
	}
	bc.Broker.Close()
}

func (bc *brokerConsumer) sendError(topic string, partition int32, err error) {
	select {
	case <-bc.tomb.Dying():
	default:
		bc.consumer.sendEvent(&ConsumerEvent{Err: err, Topic: topic, Partition: partition})
	}
}

func (bc *brokerConsumer) fetchMessages(tps []*consumerTP) []*consumerTP {
	request := new(FetchRequest)
	request.MinBytes = bc.config.MinFetchSize
	request.MaxWaitTime = bc.config.MaxWaitTime
	for _, tp := range tps {
		request.AddBlock(tp.Topic, tp.Partition, tp.Offset, tp.FetchSize)
	}

	response, err := bc.Broker.Fetch(bc.clientID, request)

	switch {
	case err == nil:
		break
	case err == EncodingError:
		bc.sendError("", -1, err)
		return tps
	default:
		for _, tp := range tps {
			bc.delayReenqueue(tp, 500*time.Millisecond)
		}
		return nil
	}

tploop:
	for _, tp := range tps {
		block := response.GetBlock(tp.Topic, tp.Partition)
		if block == nil {
			bc.sendError(tp.Topic, tp.Partition, IncompleteResponse)
			continue
		}

		switch block.Err {
		case NoError:
		case UnknownTopicOrPartition, NotLeaderForPartition, LeaderNotAvailable:
			oldTps := tps
			tps = []*consumerTP{}

			// tps.reject! { |otp| otp == tp }
			for _, otp := range oldTps {
				if otp != tp {
					tps = append(tps, otp)
				}
			}

			// Prevents broker spam. Leader elections are rare, so 500ms is no big deal.
			go bc.delayReenqueue(tp, 500*time.Millisecond)
			continue tploop
		default:
			bc.sendError(tp.Topic, tp.Partition, block.Err)
			continue tploop
		}

		if len(block.MsgSet.Messages) == 0 {
			if block.MsgSet.PartialTrailingMessage {
				if bc.config.MaxMessageSize == 0 {
					tp.FetchSize *= 2
				} else {
					if tp.FetchSize == bc.config.MaxMessageSize {
						bc.sendError(tp.Topic, tp.Partition, MessageTooLarge)
						continue
					} else {
						tp.FetchSize *= 2
						if tp.FetchSize > bc.config.MaxMessageSize {
							tp.FetchSize = bc.config.MaxMessageSize
						}
					}
				}
			}
		} else {
			tp.FetchSize = bc.config.DefaultFetchSize
		}

		for _, msgBlock := range block.MsgSet.Messages {
			select {
			case <-bc.tomb.Dying():
				return nil
			default:
				ev := &ConsumerEvent{
					Topic:     tp.Topic,
					Partition: tp.Partition,
					Key:       msgBlock.Msg.Key,
					Value:     msgBlock.Msg.Value,
					Offset:    msgBlock.Offset,
				}
				bc.consumer.sendEvent(ev)
				tp.Offset++
			}
		}

	}

	return tps
}

func (bc *brokerConsumer) delayReenqueue(tp *consumerTP, delay time.Duration) {
	time.Sleep(delay)
	select {
	case <-bc.tomb.Dying():
	default:
		bc.consumer.allocateConsumerTP(tp)
	}
}
