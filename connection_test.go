package kafka

import (
	"testing"
	"time"
)

func TestProduceRequest(t *testing.T) {
	c, err := Dial("localhost:9092")
	if err != nil {
		t.Fatal(err)
	}
	tp := TopicPartition{"foo", 0}

	req := &ProduceRequest{
		TopicPartition: TopicPartition{"foo", 0},
		Messages: Messages{
			[]byte("hello"),
			[]byte("there"),
		},
	}

	if err = c.Produce(req); err != nil {
		t.Fatal(err)
	}

	or := &OffsetsRequest{
		TopicPartition: tp,
		MaxNumber:      1,
		Time:           OffsetTimeLatest,
	}

	res, err := c.Offsets(or)

	if err != nil {
		t.Fatal(err)
	}

	offsets := <-res

	if offsets.Offsets == nil {
		t.Fatal("Got empty response for offsets")
	}

	if offsets.Err != nil {
		t.Fatal(offsets.Err)
	}

	if len(offsets.Offsets) != 1 {
		t.Fatal("expected 1 offset, got:", offsets)
	}

	recv := make(chan Message)

	poll := func() {
		for {
			fr := &FetchRequest{
				TopicPartitionOffset: offsets.Offsets[0],
				MaxSize:              3045,
			}

			fres, err := c.Fetch(fr)

			if err != nil {
				t.Fatal(err)
			}

			gotMsg := false

			for msg := range fres {
				if msg.Err != nil {
					t.Fatal("Error consuming", msg.Err)
				}
				t.Log("Got msg", string(msg.Message))

				gotMsg = true
				recv <- msg.Message
			}

			if gotMsg {
				close(recv)
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	go poll()

	seen := 0
	seenHello, seenThere := false, false
	for msg := range recv {
		seen++
		if string(msg) == "hello" {
			seenHello = true
		}
		if string(msg) == "there" {
			seenThere = true
		}
	}

	if !seenHello {
		t.Fatal("Did not see the hello message")
	}

	if !seenThere {
		t.Fatal("Did not see the there message")
	}

	if seen != 2 {
		t.Fatal("Expected to see 2 messages.  saw", seen)
	}

	t.Log("Pew")
}

func TestMultiFetchRequest(t *testing.T) {
	c, err := Dial("localhost:9092")
	if err != nil {
		t.Fatal(err)
	}

	tpfoo := TopicPartition{"foo", 0}
	tpbar := TopicPartition{"bar", 0}

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

	or := &OffsetsRequest{
		TopicPartition: tpfoo,
		MaxNumber:      1,
		Time:           OffsetTimeLatest,
	}

	res, err := c.Offsets(or)

	if err != nil {
		t.Fatal(err)
	}

	fooOffsets := <-res

	if fooOffsets.Offsets == nil {
		t.Fatal("Got empty response for offsets")
	}

	if fooOffsets.Err != nil {
		t.Fatal(fooOffsets.Err)
	}

	if len(fooOffsets.Offsets) != 1 {
		t.Fatal("expected 1 offset, got:", fooOffsets)
	}

	or.Topic = "bar"

	res, err = c.Offsets(or)

	if err != nil {
		t.Fatal(err)
	}

	barOffsets := <-res

	if barOffsets.Offsets == nil {
		t.Fatal("Got empty response for offsets")
	}

	if barOffsets.Err != nil {
		t.Fatal(barOffsets.Err)
	}

	if len(barOffsets.Offsets) != 1 {
		t.Fatal("expected 1 offset, got:", barOffsets)
	}

	recv := make(chan Message)

	poll := func() {
		for {
			fr := MultiFetchRequest{
				FetchRequest{
					TopicPartitionOffset: fooOffsets.Offsets[0],
					MaxSize:              3045,
				},
				FetchRequest{
					TopicPartitionOffset: barOffsets.Offsets[0],
					MaxSize:              3045,
				},
			}

			fres, err := c.MultiFetch(fr)

			if err != nil {
				t.Fatal(err)
			}

			gotMsg := false

			for msg := range fres {
					if msg.Err != nil {
						t.Fatal("Error consuming", msg.Err)
					}
					t.Log("Got msg", string(msg.Message))

					gotMsg = true
					recv <- msg.Message
			}

			if gotMsg {
				close(recv)
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	go poll()

	seen := 0
	seenHello, seenThere := false, false
	for msg := range recv {
		seen++
		if string(msg) == "hello" {
			seenHello = true
		}
		if string(msg) == "there" {
			seenThere = true
		}
	}

	if !seenHello {
		t.Fatal("Did not see the hello message")
	}

	if !seenThere {
		t.Fatal("Did not see the there message")
	}

	if seen != 4 {
		t.Fatal("Expected to see 4 messages.  saw", seen)
	}
}
