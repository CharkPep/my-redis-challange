package encoding

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var (
	TERMINATOR       = []byte("\r\n")
	BULKSTRINGNULL   = []byte("$-1\r\n")
	SimpleStringType = []byte("+")
	SimpleErrorType  = []byte("-")
	SimpleIntType    = []byte(":")
	BulkStringType   = []byte("$")
	ArrayType        = []byte("*")
)

type Marshaller interface {
	MarshalRESP(w io.Writer) (int, error)
}

type Unmarshaler interface {
	UnmarshalRESP(r *bufio.Reader) (int, error)
}

func peekAndAssert(r *bufio.Reader, expected []byte) error {
	peeked, err := r.Peek(len(expected))
	if err != nil {
		return err
	}

	if !bytes.Equal(peeked, expected) {
		return fmt.Errorf("expected %s, got %s", expected, peeked)
	}
	return nil

}

type SimpleString struct {
	S string
}

func (s SimpleString) MarshalRESP(w io.Writer) (int, error) {
	buff := make([]byte, 0, 64)
	buff = append(buff, SimpleStringType...)
	buff = append(buff, []byte(s.S)...)
	buff = append(buff, TERMINATOR...)
	return w.Write(buff)
}
func (s *SimpleString) UnmarshalRESP(r *bufio.Reader) (n int, err error) {
	if err = peekAndAssert(r, SimpleStringType); err != nil {
		return
	}
	n, err = r.Discard(len(SimpleStringType))
	if err != nil {
		return n, err
	}
	str, err := r.ReadSlice(TERMINATOR[0])
	if err != nil {
		return
	}
	n += len(str)
	s.S = string(str[:len(str)-1])
	if _, err = r.Discard(len(TERMINATOR) - 1); err != nil {
		return
	}
	n += len(TERMINATOR) - 1
	return
}

func (s *SimpleString) String() string {
	return s.S
}

type SimpleError struct {
	E string
}

func (e SimpleError) MarshalRESP(w io.Writer) (int, error) {
	buff := make([]byte, 0, 16)
	buff = append(buff, SimpleErrorType...)
	buff = append(buff, []byte(e.E)...)
	buff = append(buff, TERMINATOR...)
	return w.Write(buff)
}

func (e *SimpleError) UnmarshalRESP(r *bufio.Reader) (n int, err error) {
	if err = peekAndAssert(r, SimpleErrorType); err != nil {
		return
	}

	n, err = r.Discard(len(SimpleErrorType))
	str, err := r.ReadSlice(TERMINATOR[0])
	if err != nil {
		return
	}
	n += len(str)
	e.E = string(str[:len(str)-1])
	if _, err = r.Discard(len(TERMINATOR) - 1); err != nil {
		return
	}
	n += len(TERMINATOR) - 1
	return
}

func (e SimpleError) Error() string {
	return fmt.Sprintf("Redis error: %s", e.E)
}

type SimpleInt struct {
	I int64
}

func (i SimpleInt) MarshalRESP(w io.Writer) (int, error) {
	buff := make([]byte, 0, 16)
	buff = append(buff, SimpleIntType...)
	buff = strconv.AppendInt(buff, i.I, 10)
	buff = append(buff, TERMINATOR...)
	return w.Write(buff)
}

func (i *SimpleInt) UnmarshalRESP(r *bufio.Reader) (n int, err error) {
	if err = peekAndAssert(r, SimpleIntType); err != nil {
		return
	}

	n, err = r.Discard(len(SimpleIntType))
	str, err := r.ReadSlice(TERMINATOR[0])
	if err != nil {
		return
	}

	n += len(str)
	val, err := strconv.ParseInt(string(str[:len(str)-1]), 10, 64)
	if err != nil {
		return
	}
	i.I = val
	if _, err = r.Discard(len(TERMINATOR) - 1); err != nil {
		return
	}
	n += len(TERMINATOR) - 1
	return
}

// BulkString Represents RESP binary string
type BulkString struct {
	S []byte
	// Encode nil as $-1\r\n, if false and S is nil, will marshal return an error
	EncodeNil bool
}

func (b BulkString) MarshalRESP(w io.Writer) (int, error) {
	// Marshal nil BulkString as $-1\r\n?
	if b.EncodeNil && b.S == nil {
		return w.Write(BULKSTRINGNULL)
	}

	if b.S == nil {
		return 0, fmt.Errorf("nil BulkString with EncodeNil=false")
	}

	buff := make([]byte, 0, 16)
	buff = append(buff, BulkStringType...)
	buff = strconv.AppendInt(buff, int64(len(b.S)), 10)
	buff = append(buff, TERMINATOR...)
	buff = append(buff, b.S...)
	buff = append(buff, TERMINATOR...)
	return w.Write(buff)

}

func (b *BulkString) String() string {
	return string(b.S)
}

func (b *BulkString) UnmarshalRESP(r *bufio.Reader) (n int, err error) {
	if err = peekAndAssert(r, BulkStringType); err != nil {
		return
	}

	n, err = r.Discard(len(BulkStringType))
	str, err := r.ReadSlice(TERMINATOR[0])
	if err != nil {
		return n, err
	}
	n += len(str)
	length, err := strconv.ParseInt(string(str[:len(str)-1]), 10, 64)
	if err != nil {
		return n, err
	}

	discarded, err := r.Discard(len(TERMINATOR) - 1)
	if err != nil {
		return n, err
	}
	n += discarded
	if length == -1 {
		b.EncodeNil = true
		return n, nil
	}
	b.S = make([]byte, length)
	read, err := io.ReadFull(r, b.S)
	if err != nil {
		return n, err
	}
	n += read
	_, err = r.Discard(len(TERMINATOR))
	return n + len(TERMINATOR), err
}

type Array struct {
	A []Marshaller
}

func (a *Array) Append(m Marshaller) {
	a.A = append(a.A, m)
}

func (a *Array) AppendArray(arr *Array) {
	a.A = append(a.A, arr.A...)
}

func (a Array) MarshalRESP(w io.Writer) (int, error) {
	buff := make([]byte, 0, 64)
	buff = append(buff, ArrayType...)
	buff = strconv.AppendInt(buff, int64(len(a.A)), 10)
	buff = append(buff, TERMINATOR...)
	elementsBuff := bytes.NewBuffer(make([]byte, 0, 64))
	for _, v := range a.A {
		n, err := v.MarshalRESP(elementsBuff)
		if err != nil {
			return n, err
		}
	}
	buff = append(buff, elementsBuff.Bytes()...)
	return w.Write(buff)
}

func (a *Array) UnmarshalRESP(r *bufio.Reader) (n int, err error) {
	if err = peekAndAssert(r, ArrayType); err != nil {
		return n, err
	}

	n, err = r.Discard(len(ArrayType))
	str, err := r.ReadSlice(TERMINATOR[0])
	if err != nil {
		return n, err
	}
	n += len(str)
	length, err := strconv.ParseInt(string(str[:len(str)-1]), 10, 64)
	if err != nil {
		return n, err
	}
	if _, err = r.Discard(len(TERMINATOR) - 1); err != nil {
		return n, err
	}
	n += len(TERMINATOR) - 1
	a.A = make([]Marshaller, length)
	for i := 0; i < int(length); i++ {
		var resp Any
		read, err := resp.UnmarshalRESP(r)
		if err != nil {
			return n, err
		}
		n += read
		a.A[i] = resp.I.(Marshaller)
	}
	return n, nil
}

type Any struct {
	I                   interface{}
	EncodeBulkStringNil bool
}

func convertAnyIntToInt64(a interface{}) int64 {
	switch v := a.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	}
	return 0
}

func (a Any) MarshalRESP(w io.Writer) (n int, err error) {
	switch v := a.I.(type) {
	case Marshaller:
		return v.MarshalRESP(w)
	case fmt.Stringer:
		return SimpleString{S: v.String()}.MarshalRESP(w)
	case int, int8, int16, int32, int64:
		i := SimpleInt{I: convertAnyIntToInt64(v)}
		return i.MarshalRESP(w)
	case string:
		s := SimpleString{S: v}
		return s.MarshalRESP(w)
	case error:
		e := SimpleError{E: v.Error()}
		return e.MarshalRESP(w)
	case []byte:
		b := BulkString{S: v, EncodeNil: a.EncodeBulkStringNil}
		return b.MarshalRESP(w)
	case nil:
		b := BulkString{S: nil, EncodeNil: a.EncodeBulkStringNil}
		return b.MarshalRESP(w)
	case []interface{}:
		var arrayPrefix = make([]byte, 0, 32)
		arrayPrefix = append(arrayPrefix, ArrayType...)
		arrayPrefix = strconv.AppendInt(arrayPrefix, int64(len(v)), 10)
		arrayPrefix = append(arrayPrefix, TERMINATOR...)
		arrayBuff := bytes.NewBuffer(arrayPrefix)
		var read int
		for _, i := range v {
			resp := Any{I: i}
			if read, err = resp.MarshalRESP(arrayBuff); err != nil {
				return n, err
			}
			n += read
		}
		return w.Write(arrayBuff.Bytes())
	case []Marshaller:
		arr := Array{A: v}
		return arr.MarshalRESP(w)
	}

	return n, fmt.Errorf("unknown RESP type: %T", a.I)
}

func (a *Any) UnmarshalRESP(r *bufio.Reader) (n int, err error) {
	peeked, err := r.Peek(1)
	if err != nil {
		return n, err
	}
	switch peeked[0] {
	case SimpleStringType[0]:
		var s SimpleString
		n, err = s.UnmarshalRESP(r)
		if err != nil {
			return n, err
		}
		a.I = s
	case SimpleErrorType[0]:
		var e SimpleError
		n, err = e.UnmarshalRESP(r)
		if err != nil {
			return n, err
		}
		a.I = e
	case SimpleIntType[0]:
		var i SimpleInt
		n, err = i.UnmarshalRESP(r)
		if err != nil {
			return n, err
		}
		a.I = i
	case BulkStringType[0]:
		var b BulkString
		n, err = b.UnmarshalRESP(r)
		if err != nil {
			return n, err
		}
		a.I = b
	case ArrayType[0]:
		var arr Array
		n, err = arr.UnmarshalRESP(r)
		if err != nil {
			return n, err
		}
		a.I = arr
	default:
		return n, fmt.Errorf("unknown RESP type: %s", peeked)
	}
	return n, nil
}

// TODO
func (arr *Array) String() string {
	str := strings.Builder{}
	str.WriteString("[")
	for _, v := range arr.A {
		if r, ok := v.(fmt.Stringer); ok {
			str.WriteString(r.String())
		} else {
			str.WriteString(fmt.Sprintf("%s", v))
		}
	}
	str.WriteString("]")
	return str.String()
}
