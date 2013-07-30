package protocol

import enc "sarama/encoding"
import "sarama/types"

type OffsetCommitResponse struct {
	ClientID string
	Errors   map[string]map[int32]*types.KError
}

func (r *OffsetCommitResponse) Decode(pd enc.PacketDecoder) (err error) {
	r.ClientID, err = pd.GetString()
	if err != nil {
		return err
	}

	numTopics, err := pd.GetArrayLength()
	if err != nil {
		return err
	}

	r.Errors = make(map[string]map[int32]*types.KError, numTopics)
	for i := 0; i < numTopics; i++ {
		name, err := pd.GetString()
		if err != nil {
			return err
		}

		numErrors, err := pd.GetArrayLength()
		if err != nil {
			return err
		}

		r.Errors[name] = make(map[int32]*types.KError, numErrors)

		for j := 0; j < numErrors; j++ {
			id, err := pd.GetInt32()
			if err != nil {
				return err
			}

			tmp, err := pd.GetInt16()
			if err != nil {
				return err
			}
			tmp2 := types.KError(tmp)
			r.Errors[name][id] = &tmp2
		}
	}

	return nil
}

func (r *OffsetCommitResponse) GetError(topic string, partition int32) *types.KError {
	if r.Errors == nil {
		return nil
	}

	if r.Errors[topic] == nil {
		return nil
	}

	return r.Errors[topic][partition]
}
