package kafka

import (
	"bytes"
	"testing"
)

// TODO check contents of messages

func TestFetch(t *testing.T) {
	buff := bytes.NewBuffer(make([]byte, 0, 256))

	fr := FetchRequest{TopicPartitionOffset{TopicPartition{"foo", 1}, 362}, 32}

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
		FetchRequest{TopicPartitionOffset{TopicPartition{"foo", 1}, 362}, 32},
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

	tp := TopicPartition{"foo", 0}
	pr := ProduceRequest{
		TopicPartition: tp,
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
	tpfoo := TopicPartition{"foo", 0}
	tpbar := TopicPartition{"bar", 0}

	pr := MultiProduceRequest{
		ProduceRequest{
			TopicPartition: tpfoo,
			Messages: Messages{
				[]byte("hello"),
				[]byte("there"),
			},
		},
		ProduceRequest{
			TopicPartition: tpbar,
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
