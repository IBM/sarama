package sarama

import (
	"bytes"
	"reflect"
	"testing"
)

func TestLegacyRecords(t *testing.T) {
	set := &MessageSet{
		Messages: []*MessageBlock{
			{
				Msg: &Message{
					Version: 1,
				},
			},
		},
	}
	r := Records{msgSet: set}

	exp, err := encode(set, nil)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := encode(&r, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, exp) {
		t.Errorf("Wrong encoding for legacy records, wanted %v, got %v", exp, buf)
	}

	set = &MessageSet{}
	r = Records{}

	err = decode(exp, set)
	if err != nil {
		t.Fatal(err)
	}
	err = decode(buf, &r)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(set, r.msgSet) {
		t.Errorf("Wrong decoding for legacy records, wanted %#+v, got %#+v", set, r.msgSet)
	}

	n := r.numRecords()
	if n != 1 {
		t.Errorf("Wrong number of records, wanted 1, got %d", n)
	}

	p := r.isPartial()
	if p {
		t.Errorf("MessageSet shouldn't have a partial trailing message")
	}
}

func TestDefaultRecords(t *testing.T) {
	batch := &RecordBatch{
		Version: 2,
		Records: []*Record{
			{
				Value: []byte{1},
			},
		},
	}

	r := Records{recordBatch: batch}

	exp, err := encode(batch, nil)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := encode(&r, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, exp) {
		t.Errorf("Wrong encoding for default records, wanted %v, got %v", exp, buf)
	}

	batch = &RecordBatch{}
	r = Records{}

	err = decode(exp, batch)
	if err != nil {
		t.Fatal(err)
	}
	err = decode(buf, &r)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(batch, r.recordBatch) {
		t.Errorf("Wrong decoding for default records, wanted %#+v, got %#+v", batch, r.recordBatch)
	}

	n := r.numRecords()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("Wrong number of records, wanted 1, got %d", n)
	}

	p := r.isPartial()
	if err != nil {
		t.Fatal(err)
	}
	if p {
		t.Errorf("RecordBatch shouldn't have a partial trailing record")
	}

	if r.recordBatch.Control {
		t.Errorf("RecordBatch shouldn't be a control batch")
	}
}
