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

	req := &ProduceRequest{
		Topic:     "foo",
		Partition: 0,
		Messages: Messages{
			[]byte("hello"),
			[]byte("there"),
		},
	}

	if err = c.Produce(req); err != nil {
		t.Fatal(err)
	}

	or := &OffsetsRequest{
		Topic:     "foo",
		Partition: 0,
		MaxNumber: 1,
		Time:      OffsetTimeLatest,
	}

	res, err := c.Offsets(or)

	if err != nil {
		t.Fatal(err)
	}

	offsets := <-res

	if offsets == nil {
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
				Topic:     "foo",
				Offset:    offsets.Offsets[0],
				MaxSize:   3045,
				Partition: 0,
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
