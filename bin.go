package kafka

import (
	"encoding/binary"
	"io"
)

var networkOrder = binary.BigEndian

// First reads the length to the size pt, then reads the string to the topic
func binread(r io.Reader, data ...interface{}) error {
	for _, d := range data {
		if err := binary.Read(r, networkOrder, d); err != nil {
			return err
		}
	}
	return nil
}

func binwrite(w io.Writer, data ...interface{}) (l int64, err error) {
	for _, d := range data {
		if err := binary.Write(w, networkOrder, d); err != nil {
			return -1, err
		}
		l += int64(binary.Size(d))
	}
	return
}
