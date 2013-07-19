package protocol

import enc "sarama/encoding"

type fetchRequestBlock struct {
	fetchOffset int64
	maxBytes    int32
}

func (f *fetchRequestBlock) Encode(pe enc.PacketEncoder) error {
	pe.PutInt64(f.fetchOffset)
	pe.PutInt32(f.maxBytes)
	return nil
}

type FetchRequest struct {
	MaxWaitTime int32
	MinBytes    int32
	blocks      map[string]map[int32]*fetchRequestBlock
}

func (f *FetchRequest) Encode(pe enc.PacketEncoder) (err error) {
	pe.PutInt32(-1) // replica ID is always -1 for clients
	pe.PutInt32(f.MaxWaitTime)
	pe.PutInt32(f.MinBytes)
	err = pe.PutArrayLength(len(f.blocks))
	if err != nil {
		return err
	}
	for topic, blocks := range f.blocks {
		err = pe.PutString(topic)
		if err != nil {
			return err
		}
		err = pe.PutArrayLength(len(blocks))
		if err != nil {
			return err
		}
		for partition, block := range blocks {
			pe.PutInt32(partition)
			err = block.Encode(pe)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *FetchRequest) key() int16 {
	return 1
}

func (f *FetchRequest) version() int16 {
	return 0
}

func (f *FetchRequest) AddBlock(topic string, partition_id int32, fetchOffset int64, maxBytes int32) {
	if f.blocks == nil {
		f.blocks = make(map[string]map[int32]*fetchRequestBlock)
	}

	if f.blocks[topic] == nil {
		f.blocks[topic] = make(map[int32]*fetchRequestBlock)
	}

	tmp := new(fetchRequestBlock)
	tmp.maxBytes = maxBytes
	tmp.fetchOffset = fetchOffset

	f.blocks[topic][partition_id] = tmp
}
