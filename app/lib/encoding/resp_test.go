package encoding

import (
	"bufio"
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestMarshalSimpleString(t *testing.T) {
	type test struct {
		input    SimpleString
		expected string
	}
	tests := []test{
		{
			input:    SimpleString{"hello"},
			expected: "+hello\r\n",
		},
		{
			input:    SimpleString{""},
			expected: "+\r\n",
		},
	}
	for _, tc := range tests {
		var b bytes.Buffer
		n, err := tc.input.MarshalRESP(&b)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if n != len(tc.expected) {
			t.Errorf("i %d, got %d", len(tc.expected), n)
		}
		if b.String() != tc.expected {
			t.Errorf("i %q, got %q", tc.expected, b.String())
		}
	}
}

func TestUnmarshalSimpleString(t *testing.T) {
	type tc struct {
		input    []byte
		expected SimpleString
	}
	tests := []tc{
		{
			input:    []byte("+hello\r\n"),
			expected: SimpleString{"hello"},
		},
		{
			input:    []byte("+\r\n"),
			expected: SimpleString{""},
		},
	}
	for _, test := range tests {
		var s SimpleString
		r := bufio.NewReader(bytes.NewReader(test.input))
		n, err := s.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if n != len(test.input) {
			t.Errorf("i %d, got %d", len(test.input), n)
		}
		if s != test.expected {
			t.Errorf("i %v, got %v", test.expected, s)
		}
	}
}

func TestSimpleError_MarshalRESP(t *testing.T) {
	type test struct {
		input    SimpleError
		expected string
	}
	tests := []test{
		{
			input:    SimpleError{"error"},
			expected: "-error\r\n",
		},
		{
			input:    SimpleError{""},
			expected: "-\r\n",
		},
	}
	for _, tc := range tests {
		var b bytes.Buffer
		n, err := tc.input.MarshalRESP(&b)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if n != len(tc.expected) {
			t.Errorf("i %d, got %d", len(tc.expected), n)
		}
		if b.String() != tc.expected {
			t.Errorf("i %q, got %q", tc.expected, b.String())
		}
	}
}

func TestSimpleError_UnmarshalRESP(t *testing.T) {
	type tc struct {
		input    []byte
		expected SimpleError
	}
	tests := []tc{
		{
			input:    []byte("-error\r\n"),
			expected: SimpleError{"error"},
		},
		{
			input:    []byte("-\r\n"),
			expected: SimpleError{""},
		},
	}
	for _, test := range tests {
		var s SimpleError
		r := bufio.NewReader(bytes.NewReader(test.input))
		n, err := s.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if n != len(test.input) {
			t.Errorf("i %d, got %d", len(test.input), n)
		}
		if s != test.expected {
			t.Errorf("i %v, got %v", test.expected, s)
		}
	}
}

func TestSimpleInt_MarshalRESP(t *testing.T) {
	type test struct {
		input    SimpleInt
		expected string
	}
	tests := []test{
		{
			input:    SimpleInt{123},
			expected: ":123\r\n",
		},
		{
			input:    SimpleInt{-123},
			expected: ":-123\r\n",
		},
		{
			input:    SimpleInt{0},
			expected: ":0\r\n",
		},
	}
	for _, tc := range tests {
		var b bytes.Buffer
		n, err := tc.input.MarshalRESP(&b)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if n != len(tc.expected) {
			t.Errorf("i %d, got %d", len(tc.expected), n)
		}
		if b.String() != tc.expected {
			t.Errorf("i %q, got %q", tc.expected, b.String())
		}
	}
}

func TestSimpleInt_UnmarshalRESP(t *testing.T) {
	type tc struct {
		input    []byte
		expected SimpleInt
	}
	tests := []tc{
		{
			input:    []byte(":123\r\n"),
			expected: SimpleInt{123},
		},
		{
			input:    []byte(":-123\r\n"),
			expected: SimpleInt{-123},
		},
		{
			input:    []byte(":0\r\n"),
			expected: SimpleInt{0},
		},
	}
	for _, test := range tests {
		var s SimpleInt
		r := bufio.NewReader(bytes.NewReader(test.input))
		n, err := s.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if n != len(test.input) {
			t.Errorf("i %d, got %d", len(test.input), n)
		}
		if s != test.expected {
			t.Errorf("i %v, got %v", test.expected, s)
		}
	}
}

func TestBulkString_MarshalRESP(t *testing.T) {
	type test struct {
		input    BulkString
		expected string
	}
	tests := []test{
		{
			input:    BulkString{[]byte("hello"), false},
			expected: "$5\r\nhello\r\n",
		},
		{
			input:    BulkString{[]byte(""), false},
			expected: "$0\r\n\r\n",
		},
		{
			input:    BulkString{nil, true},
			expected: "$-1\r\n",
		},
	}
	for _, tc := range tests {
		var b bytes.Buffer
		n, err := tc.input.MarshalRESP(&b)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		if n != len(tc.expected) {
			t.Errorf("i %d, got %d", len(tc.expected), n)
		}

		if !bytes.Equal(b.Bytes(), []byte(tc.expected)) {
			t.Errorf("i %q, got %q", []byte(tc.expected), b.Bytes())
		}
	}
}

func TestBulkString_UnmarshalRESP(t *testing.T) {
	type test struct {
		input    []byte
		expected BulkString
	}

	tcs := []test{
		{
			input:    []byte("$5\r\nhello\r\n"),
			expected: BulkString{[]byte("hello"), false},
		},
		{
			input:    []byte("$0\r\n\r\n"),
			expected: BulkString{[]byte(""), false},
		},
		{
			input:    []byte("$-1\r\n"),
			expected: BulkString{nil, true},
		},
	}
	for _, tc := range tcs {
		var s BulkString
		r := bufio.NewReader(bytes.NewReader(tc.input))
		n, err := s.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if n != len(tc.input) {
			t.Errorf("i %d, got %d", len(tc.input), n)
		}
		if !AssertBulkString(s, tc.expected) {
			t.Errorf("i %v, got %v", tc.expected, s)
		}
	}
}

func AssertBulkString(a, b BulkString) bool {
	if a.EncodeNil != b.EncodeNil {
		return false
	}
	if !bytes.Equal(a.S, b.S) {
		return false
	}
	return true
}

func TestRespArray_MarshalRESP(t *testing.T) {
	type input struct {
		i Array
		o []byte
	}

	tc := []input{
		{
			i: Array{
				A: []Marshaller{
					BulkString{[]byte("foo"), false},
					SimpleString{"bar"},
					SimpleInt{-1},
				},
			},
			o: []byte("*3\r\n$3\r\nfoo\r\n+bar\r\n:-1\r\n"),
		},
	}

	for _, test := range tc {
		var b bytes.Buffer
		n, err := test.i.MarshalRESP(&b)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		if n != len(test.o) {
			t.Errorf("i %d, got %d", len(test.o), n)
		}

		if b.String() != string(test.o) {
			t.Errorf("i %q, got %q", test.o, b.String())
		}
	}
}

func TestRespArray_UnmarshalRESP(t *testing.T) {
	type input struct {
		i        []byte
		expected Array
	}

	tc := []input{
		{
			i: []byte("*3\r\n$3\r\nfoo\r\n+bar\r\n:-1\r\n"),
			expected: Array{
				A: []Marshaller{
					BulkString{[]byte("foo"), false},
					SimpleString{"bar"},
					SimpleInt{-1},
				},
			},
		},
	}

	for _, test := range tc {
		var s Array
		r := bufio.NewReader(bytes.NewReader(test.i))
		n, err := s.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if n != len(test.i) {
			t.Errorf("i %d, got %d", len(test.i), n)
		}
		if !AssertRespArray(s, test.expected) {
			t.Errorf("i %v, got %v", test.expected, s)
		}
	}
}

func AssertRespArray(a Array, b Array) bool {
	if len(a.A) != len(b.A) {
		return false
	}
	for i, val := range a.A {
		if !reflect.DeepEqual(val, b.A[i]) {
			return false
		}
	}
	return true

}

func TestSimplePrimitivesInAnyResp_UnmarshalRESP(t *testing.T) {
	type testCase struct {
		input    []byte
		expected Marshaller
	}

	tests := []testCase{
		{
			input:    []byte("+hello\r\n"),
			expected: SimpleString{"hello"},
		},
		{
			input:    []byte("-error\r\n"),
			expected: SimpleError{"error"},
		},
		{
			input:    []byte(":123\r\n"),
			expected: SimpleInt{123},
		},
		{
			input:    []byte("$5\r\nhello\r\n"),
			expected: BulkString{[]byte("hello"), false},
		},
		{
			input: []byte("*3\r\n$3\r\nfoo\r\n+bar\r\n:-1\r\n"),
			expected: Array{
				A: []Marshaller{
					BulkString{[]byte("foo"), false},
					SimpleString{"bar"},
					SimpleInt{-1},
				},
			},
		},
	}
	for _, test := range tests {
		var s Any
		r := bufio.NewReader(bytes.NewReader(test.input))
		n, err := s.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if n != len(test.input) {
			t.Errorf("i %d, got %d", len(test.input), n)
		}
		if !AssertAny(s, test.expected) {
			t.Errorf("i %v, got %v", test.expected, s)
		}
	}
}

func TestAnyResp_MarshalRESP(t *testing.T) {
	type test struct {
		input    Any
		expected string
	}
	tests := []test{
		{
			input:    Any{"simple", false},
			expected: "+simple\r\n",
		},
		{
			input:    Any{"", false},
			expected: "+\r\n",
		},
		{
			input:    Any{errors.New("RESP error"), false},
			expected: "-RESP error\r\n",
		},
		{
			input:    Any{nil, true},
			expected: "$-1\r\n",
		},
		{
			input:    Any{[]byte("hello"), true},
			expected: "$5\r\nhello\r\n",
		},
	}

	for _, tc := range tests {
		var b bytes.Buffer
		n, err := tc.input.MarshalRESP(&b)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if n != len(tc.expected) {
			t.Errorf("i %d, got %d", len(tc.expected), n)
		}
		if !bytes.Equal(b.Bytes(), []byte(tc.expected)) {
			t.Errorf("i %q, got %q", tc.expected, b.String())
		}
	}
}

func AssertAny(a Any, b Marshaller) bool {
	if reflect.DeepEqual(a.I, b) {
		return true
	}
	return false
}

func TestAnyResp_MarshalRESP2(t *testing.T) {
	type input struct {
		i []interface{}
		o string
	}
	tests := []input{
		{
			i: []interface{}{SimpleString{"foo"}, SimpleString{"bar"}, SimpleInt{123}},
			o: "*3\r\n+foo\r\n+bar\r\n:123\r\n",
		},
		{
			i: []interface{}{[]byte("foo"), "bar", 123},
			o: "*3\r\n$3\r\nfoo\r\n+bar\r\n:123\r\n",
		},
	}
	for _, test := range tests {
		var b bytes.Buffer
		n, err := Any{I: test.i}.MarshalRESP(&b)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if n != len(test.o) {
			t.Errorf("i %d, got %d", len(test.o), n)
		}
		if b.String() != test.o {
			t.Errorf("i %q, got %q", test.o, b.String())
		}
	}
}

func TestAnyResp_UnmarshalRESPAndMarshalRESP(t *testing.T) {
	type tests struct {
		input  []byte
		output Any
	}
	testCases := []tests{
		{
			input:  []byte("+hello\r\n"),
			output: Any{SimpleString{"hello"}, false},
		},
		{
			input:  []byte("-error\r\n"),
			output: Any{SimpleError{"error"}, false},
		},
		{
			input:  []byte(":123\r\n"),
			output: Any{SimpleInt{123}, false},
		},
		{
			input:  []byte("$5\r\nhello\r\n"),
			output: Any{BulkString{[]byte("hello"), false}, false},
		},
		{
			input: []byte("*3\r\n$3\r\nfoo\r\n+bar\r\n:-1\r\n"),
			output: Any{Array{
				A: []Marshaller{
					BulkString{[]byte("foo"), false},
					SimpleString{"bar"},
					SimpleInt{-1},
				},
			}, false},
		},
	}
	// Little bit brainfuck, basically what we want to do is check whether
	// Marshaling and Unmarshaling gives the same data
	for _, test := range testCases {
		var marshaled Any
		marshaled.I = test.output.I
		buff := bytes.NewBuffer([]byte{})
		n, err := marshaled.MarshalRESP(buff)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if n != len(test.input) {
			t.Errorf("expected %d, got %d", n, len(test.input))
		}
		if buff.String() != string(test.input) {
			t.Errorf("i %q, got %q", test.input, buff.String())
		}

		tUnmarshaler := reflect.TypeOf(test.output)
		unmarshaled, ok := reflect.New(tUnmarshaler).Interface().(Unmarshaler)
		if !ok {
			t.Errorf("unexpected error: %s", err)
		}

		r := bufio.NewReader(bytes.NewReader(test.input))
		n, err = unmarshaled.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		val := reflect.ValueOf(unmarshaled)
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}
		if n != len(test.input) {
			t.Errorf("expected %d, got %d", n, len(test.input))
		}
		if AssertAny(val.Interface().(Any), test.output) {
			t.Errorf("i %v, got %q", test.output, val)
		}

	}
}

func TestSimpleString_UnmarshalRESP(t *testing.T) {
	type ts struct {
		input    []byte
		expected SimpleString
	}

	tests := []ts{
		{
			input:    []byte("+hello\r\n"),
			expected: SimpleString{"hello"},
		},
		{
			input:    []byte("+ hello 0 0\r\nsome other stuff\r\n"),
			expected: SimpleString{" hello 0 0"},
		},
		{
			input:    []byte("+FULLRESYNC _ 0\r\n$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\b\xbce\xfa\bused-mem°\xc4\x10\x00\xfa\baof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2"),
			expected: SimpleString{"FULLRESYNC _ 0"},
		},
	}

	for _, test := range tests {
		var s SimpleString
		r := bufio.NewReader(bytes.NewReader(test.input))
		_, err := s.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if s != test.expected {
			t.Errorf("expected %v, got %v", test.expected, s)
		}
	}
}
