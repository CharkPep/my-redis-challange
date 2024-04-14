package encoding

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// next 6 bits is the length
	RDB_6BIT = 0x00
	//next 14 bits is the length
	RDB_14BIT            = 0x01
	RDB_32BIT            = 0x02
	RDB_8BIT_STR__AS_INT = 0x03
	RDB_16BIT_STR_AS_INT = 0x07
	RDB_32BIT_STR_AS_INT = 0x0b
)

func Decode(r *bufio.Reader) (n uint32, isIntString bool, err error) {
	b, err := r.Peek(1)
	if err != nil {
		return 0, false, err
	}

	if b[0]&0xc0 == 0 {
		if _, err := r.Discard(1); err != nil {
			return 0, false, err
		}

		return uint32(b[0]), false, nil
	}

	if b[0]&0x40 != 0 && b[0]&0x80 == 0 {
		b, err = r.Peek(2)
		if err != nil {
			return 0, false, err
		}
		if _, err := r.Discard(2); err != nil {
			return 0, false, err
		}

		return uint32(binary.LittleEndian.Uint16(b) << 2), false, nil
	}

	if b[0]&0x40 == 0 && b[0]&0x80 != 0 {
		b, err = r.Peek(5)
		if _, err := r.Discard(5); err != nil {
			return 0, false, err
		}

		if err != nil {
			return 0, false, err
		}

		return binary.LittleEndian.Uint32(b[1:]), false, nil
	}

	switch b[0] ^ 0xc0 {
	case 0:
		if _, err := r.Discard(1); err != nil {
			return 0, false, err
		}
		b, err = r.Peek(1)
		if err != nil {
			return 0, false, err
		}
		if _, err := r.Discard(1); err != nil {
			return 0, false, err
		}

		return uint32(b[0]), true, nil
	case 1:
		if _, err := r.Discard(1); err != nil {
			return 0, true, err
		}

		b, err = r.Peek(2)
		if err != nil {
			return 0, false, err
		}
		if _, err := r.Discard(2); err != nil {
			return 0, false, err
		}
		return uint32(binary.LittleEndian.Uint16(b)), true, nil
	case 2:
		if _, err := r.Discard(1); err != nil {
			return 0, false, err
		}
		b, err = r.Peek(4)
		if err != nil {
			return 0, false, err
		}
		if _, err := r.Discard(4); err != nil {
			return 0, false, err
		}
		return binary.LittleEndian.Uint32(b), true, nil
	}

	return 0, false, fmt.Errorf("unknown encoding")
}

// Encode encodes a number n in little endian with the specified format and writes it to w.
// As length encoding has 6, 14 bit, for 6 bit n will be shifted by 2 bits
// and for 14 bit n will be shifted by 2 bits and then written in little endian
func Encode(format byte, n uint32, w io.Writer) (int, error) {
	switch format {
	case 0x00:
		n <<= 2
		n |= uint32(format)
		return w.Write([]byte{byte(n)})
	case 0x01:
		n <<= 2
		b := make([]byte, 2)
		binary.LittleEndian.PutUint16(b, uint16(n))
		b[0] |= format
		return w.Write(b)
	case 0x02:
		b := make([]byte, 5)
		b[0] = format
		binary.LittleEndian.PutUint32(b[1:], n)
		return w.Write(b)
	case 0x03:
		switch format >> 2 {
		case 0:
			b := make([]byte, 2)
			b[0] = format
			b[1] = byte(n)
			return w.Write(b)
		case 1:
			b := make([]byte, 3)
			b[0] = format
			binary.LittleEndian.PutUint16(b[1:], uint16(n))
			return w.Write(b)
		case 2:
			b := make([]byte, 5)
			b[0] = format
			binary.LittleEndian.PutUint32(b[1:], n)
			return w.Write(b)
		}
	}

	return 0, fmt.Errorf("unknown encoding")
}
