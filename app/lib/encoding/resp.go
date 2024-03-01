package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

var (
	TERMINATOR       = []byte("\r\n")
	BULK_STRING_NULL = []byte("$-1\r\n")
	SimpleStringType = []byte("+")
	SimpleErrorType  = []byte("-")
	SimpleIntType    = []byte(":")
	BulkStringType   = []byte("$")
	RespArrayType    = []byte("*")
)

type RespMarshaler interface {
	MarshalRESP(w io.Writer) error
}

type RespUnmarshaler interface {
	UnmarshalRESP(r *bufio.Reader) error
}

type SimpleString struct {
	S string
}

func (s SimpleString) MarshalRESP(w io.Writer) error {
	buff := make([]byte, 0, 16)
	buff = append(buff, SimpleStringType...)
	buff = append(buff, []byte(s.S)...)
	buff = append(buff, TERMINATOR...)
	_, err := w.Write(buff)
	return err
}

func peekAndAssert(r *bufio.Reader, expected []byte) error {
	peeked, err := r.Peek(len(expected))
	if err != nil {
		return err
	}
	if string(peeked) != string(expected) {
		return fmt.Errorf("input %s, got %s", expected, peeked)
	}
	return nil

}

func (s *SimpleString) UnmarshalRESP(r *bufio.Reader) error {
	if err := peekAndAssert(r, SimpleStringType); err != nil {
		return err
	}

	_, err := r.Discard(len(SimpleStringType))
	str, err := r.ReadSlice(TERMINATOR[0])
	if err != nil {
		return err
	}

	s.S = string(str[:len(str)-1])
	if _, err = r.Discard(len(TERMINATOR) - 1); err != nil {
		return err
	}
	return err
}

type SimpleError struct {
	E string
}

func (e SimpleError) MarshalRESP(w io.Writer) error {
	buff := make([]byte, 0, 16)
	buff = append(buff, SimpleErrorType...)
	buff = append(buff, []byte(e.E)...)
	buff = append(buff, TERMINATOR...)
	_, err := w.Write(buff)
	return err
}

func (e *SimpleError) UnmarshalRESP(r *bufio.Reader) error {
	if err := peekAndAssert(r, SimpleErrorType); err != nil {
		return err
	}

	_, err := r.Discard(len(SimpleErrorType))
	str, err := r.ReadSlice(TERMINATOR[0])
	if err != nil {
		return err
	}

	e.E = string(str[:len(str)-1])
	return err
}

func (e SimpleError) Error() string {
	return fmt.Sprintf("Redis error: %s", e.E)
}

type SimpleInt struct {
	I int64
}

func (i SimpleInt) MarshalRESP(w io.Writer) error {
	buff := make([]byte, 0, 16)
	buff = append(buff, SimpleIntType...)
	buff = strconv.AppendInt(buff, i.I, 10)
	buff = append(buff, TERMINATOR...)
	_, err := w.Write(buff)
	return err
}

func (i *SimpleInt) UnmarshalRESP(r *bufio.Reader) error {
	if err := peekAndAssert(r, SimpleIntType); err != nil {
		return err
	}

	_, err := r.Discard(len(SimpleIntType))
	str, err := r.ReadSlice(TERMINATOR[0])
	if err != nil {
		return err
	}

	val, err := strconv.ParseInt(string(str[:len(str)-1]), 10, 64)
	if err != nil {
		return err
	}

	i.I = val
	return err

}

// BulkString Represents RESP binary string
type BulkString struct {
	S []byte
	// Encode nil as $-1\r\n, if false and S is nil, will return an error
	EncodeNil bool
}

func (b BulkString) MarshalRESP(w io.Writer) error {
	if b.EncodeNil && b.S == nil {
		_, err := w.Write(BULK_STRING_NULL)
		return err
	}

	if b.S == nil {
		return fmt.Errorf("nil BulkString with EncodeNil=false")
	}

	buff := make([]byte, 0, 16)
	buff = append(buff, BulkStringType...)
	buff = strconv.AppendInt(buff, int64(len(b.S)), 10)
	buff = append(buff, TERMINATOR...)
	buff = append(buff, b.S...)
	buff = append(buff, TERMINATOR...)
	_, err := w.Write(buff)
	return err
}

func (b *BulkString) UnmarshalRESP(r *bufio.Reader) error {
	if err := peekAndAssert(r, BulkStringType); err != nil {
		return err
	}

	_, err := r.Discard(len(BulkStringType))
	str, err := r.ReadSlice(TERMINATOR[0])
	if err != nil {
		return err
	}

	length, err := strconv.ParseInt(string(str[:len(str)-1]), 10, 64)
	if err != nil {
		return err
	}

	_, err = r.Discard(len(TERMINATOR) - 1)
	if err != nil {
		return err
	}

	if length == -1 {
		b.EncodeNil = true
		return nil
	}
	b.S = make([]byte, length)
	_, err = io.ReadFull(r, b.S)
	if err != nil {
		return err
	}
	_, err = r.Discard(len(TERMINATOR))
	return err
}

type RespArray struct {
	A []RespMarshaler
}

func (a RespArray) MarshalRESP(w io.Writer) error {
	buff := make([]byte, 0, 16)
	buff = append(buff, RespArrayType...)
	buff = strconv.AppendInt(buff, int64(len(a.A)), 10)
	buff = append(buff, TERMINATOR...)
	elementsBuff := bytes.NewBuffer(make([]byte, 0, 16))
	for _, v := range a.A {
		err := v.MarshalRESP(elementsBuff)
		if err != nil {
			return err
		}
	}
	buff = append(buff, elementsBuff.Bytes()...)
	fmt.Println("buff", string(buff))
	_, err := w.Write(buff)
	return err
}

func (a *RespArray) UnmarshalRESP(r *bufio.Reader) error {
	if err := peekAndAssert(r, RespArrayType); err != nil {
		return err
	}

	_, err := r.Discard(len(RespArrayType))
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
	a.A = make([]RespMarshaler, length)
	for i := 0; i < int(length); i++ {
		var any AnyResp
		err := any.UnmarshalRESP(r)
		if err != nil {
			return err
		}
		a.A[i] = any.I.(RespMarshaler)
	}
	return nil
}

type AnyResp struct {
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

func (a AnyResp) MarshalRESP(w io.Writer) error {
	switch v := a.I.(type) {
	case RespMarshaler:
		return v.MarshalRESP(w)
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
		arrayPrefix = append(arrayPrefix, RespArrayType...)
		arrayPrefix = strconv.AppendInt(arrayPrefix, int64(len(v)), 10)
		arrayPrefix = append(arrayPrefix, TERMINATOR...)
		arrayBuff := bytes.NewBuffer(arrayPrefix)
		for _, i := range v {
			any := AnyResp{I: i}
			any.MarshalRESP(arrayBuff)
		}
		_, err := w.Write(arrayBuff.Bytes())
		return err
	case []RespMarshaler:
		fmt.Println("array of RespMarshaler")
		arr := RespArray{A: v}
		return arr.MarshalRESP(w)
	}

	return fmt.Errorf("unknown RESP type: %T", a.I)
}

func (a *AnyResp) UnmarshalRESP(r *bufio.Reader) error {
	peeked, err := r.Peek(1)
	if err != nil {
		return err
	}
	switch peeked[0] {
	case SimpleStringType[0]:
		var s SimpleString
		err = s.UnmarshalRESP(r)
		if err != nil {
			return err
		}
		a.I = s
	case SimpleErrorType[0]:
		var e SimpleError
		err = e.UnmarshalRESP(r)
		if err != nil {
			return err
		}
		a.I = e
	case SimpleIntType[0]:
		var i SimpleInt
		err = i.UnmarshalRESP(r)
		if err != nil {
			return err
		}
		a.I = i
	case BulkStringType[0]:
		var b BulkString
		err = b.UnmarshalRESP(r)
		if err != nil {
			return err
		}
		a.I = b
	case RespArrayType[0]:
		var arr RespArray
		err = arr.UnmarshalRESP(r)
		if err != nil {
			return err
		}
		a.I = arr
	default:
		return fmt.Errorf("unknown RESP type: %s", peeked)
	}
	return nil
}
