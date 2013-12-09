package sarama

import (
	"sync"
	"time"
)

type brokerConsumer struct {
	m      sync.Mutex
	intake chan *consumerTP
	Broker *Broker
}

func newBrokerConsumer(m *Consumer, broker *Broker) {
	defer m.wg.Done()
	m.m.RLock()
	br := m.brokerConsumers[broker]
	m.m.RUnlock()

	defer m.cleanupBrokerConsumer(br)

	var tps []*consumerTP
	for {
		select {
		case <-m.tomb.Dying():
			return
		case tp := <-br.intake:
			br.m.Lock()
			tps = append(tps, tp)
			br.m.Unlock()
		default:
			if len(tps) == 0 {
				return
			}
			tps = m.fetchMessages(broker, tps)
		}
	}
}

func (m *Consumer) cleanupBrokerConsumer(br *brokerConsumer) {
	m.m.Lock()
	close(br.intake)
	delete(m.brokerConsumers, br.Broker)
	m.m.Unlock()
}

func (m *Consumer) fetchMessages(broker *Broker, tps []*consumerTP) []*consumerTP {
	request := new(FetchRequest)
	request.MinBytes = m.config.MinFetchSize
	request.MaxWaitTime = m.config.MaxWaitTime
	for _, tp := range tps {
		request.AddBlock(tp.Topic, tp.Partition, tp.Offset, tp.FetchSize)
	}

	response, err := broker.Fetch(m.client.id, request)

	select {
	case <-m.tomb.Dying():
		return nil
	default:
	}

	switch {
	case err == nil:
		break
	case err == EncodingError:
		m.sendError("", -1, err)
		return tps
	default:
		m.client.disconnectBroker(broker)
		for _, tp := range tps {
			m.delayReenqueue(tp, 500*time.Millisecond)
		}
		return nil
	}

tploop:
	for _, tp := range tps {
		block := response.GetBlock(tp.Topic, tp.Partition)
		if block == nil {
			m.sendError(tp.Topic, tp.Partition, IncompleteResponse)
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
			go m.delayReenqueue(tp, 500*time.Millisecond)
			continue tploop
		default:
			m.sendError(tp.Topic, tp.Partition, block.Err)
			continue tploop
		}

		if len(block.MsgSet.Messages) == 0 {
			if block.MsgSet.PartialTrailingMessage {
				if m.config.MaxMessageSize == 0 {
					tp.FetchSize *= 2
				} else {
					if tp.FetchSize == m.config.MaxMessageSize {
						m.sendError(tp.Topic, tp.Partition, MessageTooLarge)
						continue
					} else {
						tp.FetchSize *= 2
						if tp.FetchSize > m.config.MaxMessageSize {
							tp.FetchSize = m.config.MaxMessageSize
						}
					}
				}
			}
		} else {
			tp.FetchSize = m.config.DefaultFetchSize
		}

		for _, msgBlock := range block.MsgSet.Messages {
			select {
			case <-m.tomb.Dying():
				return nil
			case m.events <- &ConsumerEvent{
				Topic:     tp.Topic,
				Partition: tp.Partition,
				Key:       msgBlock.Msg.Key,
				Value:     msgBlock.Msg.Value,
				Offset:    msgBlock.Offset,
			}:
				tp.Offset++
			}
		}

	}

	return tps
}
