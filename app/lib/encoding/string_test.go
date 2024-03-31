package encoding

import (
	"bufio"
	"bytes"
	"testing"
)

func TestDecodeString(t *testing.T) {
	type tt struct {
		r *bufio.Reader
		e string
	}

	tests := []tt{
		{bufio.NewReader(bytes.NewReader([]byte{RDB_6BIT | 0x04, byte('1')})), "1"},
		{bufio.NewReader(bytes.NewReader([]byte{RDB_14BIT | 0x10, 0x00, '1', '2', '3', '4'})), "1234"},
	}

	for i, test := range tests {
		tc := test
		t.Logf("Test case %d\n", i)
		t.Run("", func(t *testing.T) {
			t.Parallel()
			got, err := DecodeString(tc.r)
			if err != nil {
				t.Errorf("DecodeString() error = %v", err)
				return
			}

			if got != tc.e {
				t.Errorf("DecodeString() = %v, want %v", got, tc.e)
			}
		})
	}
}
