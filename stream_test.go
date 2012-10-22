package kafka

import (
	"log"
	"testing"
)

func TestStream(t *testing.T) {
	c, err := Dial("localhost:9092")
	if err != nil {
		t.Fatal(err)
	}
	c2, err := Dial("localhost:9092")
	if err != nil {
		t.Fatal(err)
	}

	tpfoo := TopicPartition{"foo2", 0}
	tpbar := TopicPartition{"bar2", 0}

	s, err := NewKafkaStreamWithOffsets(c2, []TopicPartition{tpfoo, tpbar}, OffsetTimeLatest)
	if err != nil {
		t.Fatal(err)
	}

	req := MultiProduceRequest{
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
				[]byte("bar-hello"),
				[]byte("bar-there"),
			},
		},
	}

	if err = c.MultiProduce(req); err != nil {
		t.Fatal(err)
	}

	seenMessages := make(map[string]bool)

	i := 0
	for msg := range s.Ch {
		log.Println("Got msg", string(msg.Message))
		seenMessages[string(msg.Message)] = true
		i += 1
		if i == 4 {
			break
		}
	}

	switch {
	case !seenMessages["hello"]:
		t.Fatal("Missing message hello")
	case !seenMessages["there"]:
		t.Fatal("Missing message there")
	case !seenMessages["bar-hello"]:
		t.Fatal("Missing message bar-hello")
	case !seenMessages["bar-there"]:
		t.Fatal("Missing message bar-there")
	}

	req2 := MultiProduceRequest{
		ProduceRequest{
			TopicPartition: tpfoo,
			Messages: Messages{
				[]byte("hellos"),
				[]byte("theres"),
			},
		},
		ProduceRequest{
			TopicPartition: tpbar,
			Messages: Messages{
				[]byte("bar-hellos"),
				[]byte("bar-theres"),
			},
		},
	}

	if err = c.MultiProduce(req2); err != nil {
		t.Fatal(err)
	}

	seenMessages = make(map[string]bool)

	i = 0
	for msg := range s.Ch {
		seenMessages[string(msg.Message)] = true
		i += 1
		if i == 4 {
			break
		}
	}
	switch {
	case !seenMessages["hellos"]:
		t.Fatal("Missing message hellos")
	case !seenMessages["theres"]:
		t.Fatal("Missing message theres")
	case !seenMessages["bar-hellos"]:
		t.Fatal("Missing message bar-hellos")
	case !seenMessages["bar-theres"]:
		t.Fatal("Missing message bar-theres")
	}
}
