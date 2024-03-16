package lib

import (
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"testing"
)

func TestRequest_GetCommand(t *testing.T) {
	type testCases struct {
		input    []resp.Marshaller
		expected string
	}
	r := NewRouter()
	tests := []testCases{
		{
			input:    []resp.Marshaller{resp.SimpleString{"hello"}},
			expected: "hello",
		},
		{
			input:    []resp.Marshaller{resp.BulkString{S: []byte("hello")}},
			expected: "hello",
		},
	}
	for _, test := range tests {
		result, _ := r.getCommand(&test.input)
		if result != test.expected {
			t.Fatalf("expected %v, got %q", test.expected, result)
		}
	}
}
