//go:build go1.18 && !functional

package sarama

import (
	"bytes"
	"testing"
)

func FuzzDecodeEncodeProduceRequest(f *testing.F) {
	for _, seed := range [][]byte{
		produceRequestEmpty,
		produceRequestHeader,
		produceRequestOneMessage,
		produceRequestOneRecord,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		for i := int16(0); i < 8; i++ {
			req := &ProduceRequest{}
			err := versionedDecode(in, req, i, nil)
			if err != nil {
				continue
			}
			out, err := encode(req, nil)
			if err != nil {
				t.Logf("%v: encode: %v", in, err)
				continue
			}
			if !bytes.Equal(in, out) {
				t.Logf("%v: not equal after round trip: %v", in, out)
			}
		}
	})
}

func FuzzDecodeEncodeFetchRequest(f *testing.F) {
	for _, seed := range [][]byte{
		fetchRequestNoBlocks,
		fetchRequestWithProperties,
		fetchRequestOneBlock,
		fetchRequestOneBlockV4,
		fetchRequestOneBlockV11,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		for i := int16(0); i < 11; i++ {
			req := &FetchRequest{}
			err := versionedDecode(in, req, i, nil)
			if err != nil {
				continue
			}
			out, err := encode(req, nil)
			if err != nil {
				t.Logf("%v: encode: %v", in, err)
				continue
			}
			if !bytes.Equal(in, out) {
				t.Logf("%v: not equal after round trip: %v", in, out)
			}
		}
	})
}
