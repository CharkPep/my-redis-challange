package resp_test

import (
	"bufio"
	"bytes"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/persistence"
	"testing"
)

func TestRdb_UnmarshalRESP(t *testing.T) {
	type tt struct {
		input    []byte
		expected resp.Rdb
	}
	empty := persistence.GetEmpty()
	emptyRdb := bytes.NewBuffer(make([]byte, 0, 128))
	empty.MarshalRESP(emptyRdb)
	tests := []tt{
		{
			input:    emptyRdb.Bytes(),
			expected: *empty,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			var r resp.Rdb
			if err := r.UnmarshalRESP(bufio.NewReader(bytes.NewBuffer(test.input))); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if !bytes.Equal(r.Content, test.expected.Content) {
				t.Fatalf("expected %q, got %q", test.expected.Content, r.Content)
			}
		})
	}
}
