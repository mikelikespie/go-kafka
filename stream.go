package kafka

import (
	"time"
)

type topicPartitionOffsetMap map[string]map[Partition]Offset

// These are slightly different than the java api.  They encompass multiple topics
type KafkaStream struct {
	offsets topicPartitionOffsetMap
	c       *SimpleConsumer
	Ch      FetchResponseChan
}

func NewKafkaStreamWithOffsets(c *SimpleConsumer, targets []TopicPartition, startTime OffsetTime) (s *KafkaStream, err error) {
	newT := make([]TopicPartitionOffset, len(targets))
	offReq := OffsetsRequest{
		Time:      startTime,
		MaxNumber: 1,
	}

	resChans := make([]OffsetsResponseChan, len(targets))
	// Let's pipeline these requests

	for i, t := range targets {
		offReq.TopicPartition = t
		if resChans[i], err = c.Offsets(offReq); err != nil {
			return nil, err
		}
	}

	// Now read the offsets
	for i, offC := range resChans {
		offRes := <-offC

		if offRes.Err != nil {
			return nil, offRes.Err
		}

		newT[i] = offRes.Offsets[0]
	}

	return NewKafkaStream(c, newT)
}

func (s *KafkaStream) partCount() (i int) {
	for _, pm := range s.offsets {
		i += len(pm)
	}
	return
}

const pollTime time.Duration = 50 * time.Millisecond

func (s *KafkaStream) poll() (err error) {
	a := time.After(pollTime)

	mfr := make(MultiFetchRequest, 0, s.partCount())
	fr := FetchRequest{}
	fr.MaxSize = 1024 * 1024
	var pm map[Partition]Offset
	for fr.Topic, pm = range s.offsets {
		for fr.Partition, fr.Offset = range pm {
			mfr = append(mfr, fr)
		}
	}

	resChan, err := s.c.MultiFetch(mfr)
	if err != nil {
		return err
	}

	for res := range resChan {
		s.Ch <- res
		if res.Err != nil {
			close(s.Ch)
			return res.Err
		}
		s.updatePartitionMap(res.TopicPartitionOffset)
	}

	<-a
	return
}

func (s *KafkaStream) pollLoop() (err error) {
	for ; err == nil; err = s.poll() {
	}
	return
}

func (s *KafkaStream) updatePartitionMap(offsets ...TopicPartitionOffset) {
	for _, o := range offsets {
		po := s.offsets[o.Topic]
		if po == nil {
			po = make(map[Partition]Offset)
			s.offsets[o.Topic] = po
		}

		po[o.Partition] = o.Offset
	}
}

func NewKafkaStream(c *SimpleConsumer, targets []TopicPartitionOffset) (s *KafkaStream, err error) {
	s = &KafkaStream{
		c:       c,
		offsets: make(topicPartitionOffsetMap),
		Ch:      make(FetchResponseChan),
	}
	s.updatePartitionMap(targets...)
	go s.pollLoop()
	return s, err
}
