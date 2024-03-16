package resp

import (
	"bufio"
	"io"
	"strconv"
)

type Rdb struct {
	Content []byte
}

func (rdb *Rdb) UnmarshalRESP(r *bufio.Reader) error {
	if err := peekAndAssert(r, []byte("$")); err != nil {
		return err
	}
	if _, err := r.Discard(len([]byte("$"))); err != nil {
		return err
	}

	str, err := r.ReadSlice(TERMINATOR[0])
	if err != nil {
		return err
	}
	length, err := strconv.ParseInt(string(str[:len(str)-1]), 10, 64)
	if err != nil {
		return err
	}

	if _, err = r.Discard(len(TERMINATOR) - 1); err != nil {
		return err
	}

	rdb.Content = make([]byte, length, length+1)
	_, err = r.Read(rdb.Content)
	if err != nil {
		return err
	}
	rdb.Content = rdb.Content[:length]
	return nil
}

func (rdb *Rdb) MarshalRESP(w io.Writer) error {
	if _, err := w.Write([]byte("$")); err != nil {
		return err
	}
	if _, err := w.Write([]byte(strconv.FormatInt(int64(len(rdb.Content)), 10))); err != nil {
		return err
	}
	if _, err := w.Write(TERMINATOR); err != nil {
		return err
	}
	if _, err := w.Write(rdb.Content); err != nil {
		return err
	}
	return nil
}
