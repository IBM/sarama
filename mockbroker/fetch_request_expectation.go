package mockbroker

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

type FetchRequestExpectation struct {
	messages []fetchResponseMessage
}

type encoder interface {
	Encode() ([]byte, error)
}

type fetchResponseMessage struct {
	topic      string
	partition  int32
	key, value encoder
	offset     uint64
}

func (e *FetchRequestExpectation) AddMessage(
	topic string, partition int32, key, value encoder, offset uint64,
) *FetchRequestExpectation {
	e.messages = append(e.messages, fetchResponseMessage{
		topic:     topic,
		partition: partition,
		key:       key,
		value:     value,
		offset:    offset,
	})
	return e
}

func (b *MockBroker) ExpectFetchRequest() *FetchRequestExpectation {
	e := &FetchRequestExpectation{}
	b.expectations <- e
	return e
}

func (e *FetchRequestExpectation) ResponseBytes() []byte {
	buf := new(bytes.Buffer)

	byTopic := make(map[string][]fetchResponseMessage)
	for _, frm := range e.messages {
		byTopic[frm.topic] = append(byTopic[frm.topic], frm)
	}

	binary.Write(buf, binary.BigEndian, uint32(len(byTopic)))
	for topic, messages := range byTopic {
		binary.Write(buf, binary.BigEndian, uint16(len(topic)))
		buf.Write([]byte(topic))

		byPartition := make(map[int32][]fetchResponseMessage)
		for _, frm := range messages {
			byPartition[frm.partition] = append(byPartition[frm.partition], frm)
		}

		binary.Write(buf, binary.BigEndian, uint32(len(byPartition)))

		for partition, messages := range byPartition {
			binary.Write(buf, binary.BigEndian, uint32(partition))
			binary.Write(buf, binary.BigEndian, uint16(0)) // error
			binary.Write(buf, binary.BigEndian, uint64(0)) // high water mark offset

			messageSetBuffer := new(bytes.Buffer)

			var maxOffset uint64

			for _, msg := range messages {
				chunk := new(bytes.Buffer)

				binary.Write(chunk, binary.BigEndian, uint8(0)) // format
				binary.Write(chunk, binary.BigEndian, uint8(0)) // attribute

				if msg.offset > maxOffset {
					maxOffset = msg.offset
				}

				if msg.key == nil {
					binary.Write(chunk, binary.BigEndian, int32(-1))
				} else {
					bytes, _ := msg.key.Encode()
					binary.Write(chunk, binary.BigEndian, int32(len(bytes)))
					chunk.Write(bytes)
				}

				if msg.value == nil {
					binary.Write(chunk, binary.BigEndian, int32(-1))
				} else {
					bytes, _ := msg.value.Encode()
					binary.Write(chunk, binary.BigEndian, int32(len(bytes)))
					chunk.Write(bytes)
				}

				cksum := crc32.ChecksumIEEE(chunk.Bytes())
				length := len(chunk.Bytes()) + 4

				binary.Write(messageSetBuffer, binary.BigEndian, uint32(length)) // message length
				binary.Write(messageSetBuffer, binary.BigEndian, uint32(cksum))  // CRC
				messageSetBuffer.Write(chunk.Bytes())
			}

			binary.Write(buf, binary.BigEndian, uint32(len(messageSetBuffer.Bytes())+8)) // msgSet size
			binary.Write(buf, binary.BigEndian, uint64(maxOffset))                       // offset
			buf.Write(messageSetBuffer.Bytes())

		}

	}

	/*
		sample response:

		0x00, 0x00, 0x00, 0x01, // number of topics
		0x00, 0x08, 'm', 'y', '_', 't', 'o', 'p', 'i', 'c', // topic name
		0x00, 0x00, 0x00, 0x01, // number of blocks for this topic
		0x00, 0x00, 0x00, 0x00, // partition id
		0x00, 0x00, // error
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // high water mark offset
		// messageSet
		0x00, 0x00, 0x00, 0x1C, // messageset size
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset
		// message
		0x00, 0x00, 0x00, 0x10, // length of message (?)
		0x23, 0x96, 0x4a, 0xf7, // CRC32
		0x00, // format
		0x00, // attribute (compression)
		0xFF, 0xFF, 0xFF, 0xFF, // key (nil)
		0x00, 0x00, 0x00, 0x02, 0x00, 0xEE, // value
	*/
	return buf.Bytes()
}
