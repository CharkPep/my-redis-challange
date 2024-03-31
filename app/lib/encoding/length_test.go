package encoding

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

func TestDecode(t *testing.T) {
	type args struct {
		r        *bufio.Reader
		isIntStr bool
		e        uint32
	}

	tests := []args{
		{bufio.NewReader(bytes.NewReader([]byte{RDB_6BIT | 0x04})), false, 0x01},
		{bufio.NewReader(bytes.NewReader([]byte{RDB_14BIT, 0xFF})), false, 0x3fc0},
		{bufio.NewReader(bytes.NewReader([]byte{RDB_32BIT, 0x01, 0x00, 0x00, 0x10})), false, 0x10000001},
		{bufio.NewReader(bytes.NewReader([]byte{RDB_8BIT_STR__AS_INT, 0x01})), true, 0x01},
		{bufio.NewReader(bytes.NewReader([]byte{RDB_16BIT_STR_AS_INT, 0x01, 0x00})), true, 0x0001},
		{bufio.NewReader(bytes.NewReader([]byte{RDB_32BIT_STR_AS_INT, 0x01, 0x00, 0x00, 0x00})), true, 0x00000001},
	}

	for i, test := range tests {
		tc := test
		t.Logf("Test case %d\n", i)
		t.Run(fmt.Sprintf("%d\n", i), func(t *testing.T) {
			t.Parallel()
			got, isIntString, err := Decode(tc.r)
			if err != nil {
				t.Errorf("Decode() error = %v", err)
				return
			}

			if isIntString != tc.isIntStr {
				t.Errorf("Decode() = %v, want %v", isIntString, tc.isIntStr)
			}

			if got != tc.e {
				t.Errorf("Decode() = %v, want %v", got, tc.e)
			}
		})
	}

}

//func TestEncode(t *testing.T) {
//	type tt struct {
//		format byte
//		n      uint32
//		e      []byte
//	}
//
//	tests := []tt{
//		{RDB_6BIT, 0x20, []byte{0x80}},
//		{RDB_14BIT, 0x4000, []byte{0x01, 0x10}},
//		{RDB_32BIT, 0x10000000, []byte{0x00, 0x00, 0x00, 0x01}},
//		{RDB_8BIT_STR__AS_INT, 0x10, []byte{0x10}},
//		{RDB_16BIT_STR_AS_INT, 0x1000, []byte{0x00, 0x01}},
//		{RDB_32BIT_STR_AS_INT, 0x10000000, []byte{0x00, 0x00, 0x00, 0x01}},
//	}
//
//	for i, tc := range tests {
//		tc := tc
//		t.Run(fmt.Sprintf("%d\n", i), func(t *testing.T) {
//			t.Parallel()
//			b := new(bytes.Buffer)
//			if _, err := Encode(tc.format, tc.n, b); err != nil {
//				t.Errorf("Encode() error = %v", err)
//				return
//			}
//
//			if !bytes.Equal(b.Bytes(), tc.e) {
//				t.Errorf("Encode() = %v, want %v", b.Bytes(), tc.e)
//			}
//		})
//	}
//
//}
