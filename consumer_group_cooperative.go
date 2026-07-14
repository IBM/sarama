package sarama

import (
	"context"
	"errors"
	"slices"
	"time"
)

// consumeCooperative keeps one session while its assignment changes
func (c *consumerGroup) consumeCooperative(ctx context.Context, topics []string, handler ConsumerGroupHandler) (err error) {
	var sess *consumerGroupSession
	defer func() {
		if sess == nil {
			return
		}
		if releaseErr := sess.release(true); releaseErr != nil && err == nil {
			err = releaseErr
		}
		c.recordSessionCause(sess)
	}()

	for {
		var held *heldAssignment
		if sess != nil {
			held = &heldAssignment{claims: sess.Claims(), generationID: sess.GenerationID()}
		}

		if sess != nil {
			sess.heartbeatMu.Lock()
		}
		res, err := c.joinSync(ctx, topics, held, c.config.Consumer.Group.Rebalance.Retry.Max)
		if sess != nil {
			if err == nil {
				// commits made while revoking must use the new generation
				sess.setGeneration(res.generationID)
			}
			sess.heartbeatMu.Unlock()
		}
		if err != nil {
			if errors.Is(err, ErrClosedClient) {
				return ErrClosedConsumerGroup
			}
			return err
		}

		if sess == nil {
			if sess, err = newConsumerGroupSession(ctx, c, res.claims, res.memberID, res.generationID, handler); err != nil {
				if errors.Is(err, ErrClosedClient) {
					return ErrClosedConsumerGroup
				}
				return err
			}
			sess.watchPartitionNumbers(res)
		} else {
			revoked, err := sess.reconcile(res)
			if err != nil {
				return err
			}
			if revoked {
				// revoked partitions become assignable after this follow-up join
				continue
			}
		}

		select {
		case <-c.closed:
			return nil
		case <-ctx.Done():
			return nil
		case <-sess.ctx.Done():
			return nil
		case cause := <-sess.rejoin:
			c.lastSessionCause = cause
		}
	}
}

func (s *consumerGroupSession) setGeneration(generationID int32) {
	s.generationID.Store(generationID)
	s.offsets.setGeneration(generationID)
}

// reconcile updates the claims of an existing cooperative session
func (s *consumerGroupSession) reconcile(res *rebalanceResult) (revokedAny bool, err error) {
	owned := s.Claims()
	revoked := diffClaims(owned, res.claims)
	added := diffClaims(res.claims, owned)

	if len(revoked) > 0 {
		if err := s.revokeClaims(revoked); err != nil {
			return false, err
		}
		s.offsets.removePartitions(revoked)
	}

	s.claims.Store(&res.claims)
	s.parent.recordAssignmentChange(countPartitions(added), countPartitions(revoked), countPartitions(res.claims))

	for topic, partitions := range added {
		if err := s.parent.client.RefreshMetadata(topic); err != nil {
			return false, err
		}
		for _, partition := range partitions {
			if err := s.managePartition(topic, partition); err != nil {
				return false, err
			}
			s.startClaim(topic, partition)
		}
	}

	s.watchPartitionNumbers(res)
	return len(revoked) > 0, nil
}

// revokeClaims waits up to the rebalance timeout for handlers to return
func (s *consumerGroupSession) revokeClaims(revoked map[string][]int32) error {
	stopping := make([]*partitionClaim, 0, len(revoked))
	for topic, partitions := range revoked {
		for _, partition := range partitions {
			tp := topicPartitionAssignment{Topic: topic, Partition: partition}
			if pc := s.running[tp]; pc != nil {
				pc.cancel(errPartitionRevoked)
				stopping = append(stopping, pc)
				delete(s.running, tp)
			}
		}
	}

	timer := time.NewTimer(s.parent.config.Consumer.Group.Rebalance.Timeout)
	defer timer.Stop()

	for _, pc := range stopping {
		select {
		case <-pc.done:
		case <-timer.C:
			s.parent.handleError(ErrRebalanceTimedOut, "", -1)
			s.cancel(ErrRebalanceTimedOut)
			return ErrRebalanceTimedOut
		}
	}
	return nil
}

// diffClaims returns the claims in a that are absent from b.
func diffClaims(a, b map[string][]int32) map[string][]int32 {
	diff := make(map[string][]int32)
	for topic, partitions := range a {
		for _, partition := range partitions {
			if !slices.Contains(b[topic], partition) {
				diff[topic] = append(diff[topic], partition)
			}
		}
	}
	return diff
}

func (s *consumerGroupSession) triggerRebalance(cause error) {
	if s.parent.protocol == RebalanceProtocolEager {
		s.cancel(cause)
		return
	}

	select {
	case s.rejoin <- cause:
	default:
	}
}
