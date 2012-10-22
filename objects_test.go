package kafka

import (
	"bytes"
	"testing"
)

// TODO check contents of messages

func TestFetch(t *testing.T) {
	buff := bytes.NewBuffer(make([]byte, 0, 256))

	fr := FetchRequest{
		Topic:     "foo",
		Offset:    362,
		MaxSize:   32,
		Partition: 1,
	}

	written, err := fr.WriteTo(buff)
	if err != nil {
		t.Fatal(err)
	}

	if written != int64(fr.Len()) {
		t.Error("Written and length are not the same. wrote:", written, "expected:", fr.Len())
	}

	if written != int64(buff.Len()) {
		t.Error("Written and  bufferlength are not the same. wrote:", written, "expected:", buff.Len())
	}
}

func TestMultiFetch(t *testing.T) {

	buff := bytes.NewBuffer(make([]byte, 0, 256))

	fr := MultiFetchRequest{
		FetchRequest{
			Topic:     "foo",
			Offset:    362,
			MaxSize:   32,
			Partition: 1,
		},
	}

	written, err := fr.WriteTo(buff)
	if err != nil {
		t.Fatal(err)
	}

	if written != int64(fr.Len()) {
		t.Error("Written and length are not the same. wrote:", written, "expected:", fr.Len())
	}

	if written != int64(buff.Len()) {
		t.Error("Written and  bufferlength are not the same. wrote:", written, "expected:", buff.Len())
	}
}

func TestProduce(t *testing.T) {
	buff := bytes.NewBuffer(make([]byte, 0, 256))

	pr := ProduceRequest{
		Topic:     "foo",
		Partition: 1,
		Messages: Messages{
			[]byte("hello"),
			[]byte("there"),
		},
	}

	written, err := pr.WriteTo(buff)
	if err != nil {
		t.Fatal(err)
	}

	if written != int64(pr.Len()) {
		t.Error("Written and length are not the same. wrote:", written, "expected:", pr.Len())
	}

	if written != int64(buff.Len()) {
		t.Error("Written and  bufferlength are not the same. wrote:", written, "expected:", buff.Len())
	}
}

func TestMultiProduce(t *testing.T) {
	buff := bytes.NewBuffer(make([]byte, 0, 256))

	pr := MultiProduceRequest{
		ProduceRequest{
			Topic:     "bar",
			Partition: 2,
			Messages: Messages{
				[]byte("hello"),
				[]byte("there"),
			},
		},
		ProduceRequest{
			Topic:     "foo",
			Partition: 1,
			Messages: Messages{
				[]byte("hello"),
				[]byte("there"),
			},
		},
	}

	written, err := pr.WriteTo(buff)
	if err != nil {
		t.Fatal(err)
	}

	if written != int64(pr.Len()) {
		t.Error("Written and length are not the same. wrote:", written, "expected:", pr.Len())
	}

	if written != int64(buff.Len()) {
		t.Error("Written and  bufferlength are not the same. wrote:", written, "expected:", buff.Len())
	}
}
