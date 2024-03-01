package resp

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
		err := tc.input.MarshalRESP(&b)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if b.String() != tc.expected {
			t.Errorf("input %q, got %q", tc.expected, b.String())
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
		err := s.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if s != test.expected {
			t.Errorf("input %v, got %v", test.expected, s)
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
		err := tc.input.MarshalRESP(&b)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if b.String() != tc.expected {
			t.Errorf("input %q, got %q", tc.expected, b.String())
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
		err := s.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if s != test.expected {
			t.Errorf("input %v, got %v", test.expected, s)
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
		err := tc.input.MarshalRESP(&b)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if b.String() != tc.expected {
			t.Errorf("input %q, got %q", tc.expected, b.String())
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
		err := s.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if s != test.expected {
			t.Errorf("input %v, got %v", test.expected, s)
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
		err := tc.input.MarshalRESP(&b)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if !CompareTwoByteSlices(b.Bytes(), []byte(tc.expected)) {
			t.Errorf("input %q, got %q", []byte(tc.expected), b.Bytes())
		}
	}
}

func CompareTwoByteSlices(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, val := range a {
		if val != b[i] {
			return false
		}
	}
	return true
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
		err := s.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if !AssertBulkString(s, tc.expected) {
			t.Errorf("input %v, got %v", tc.expected, s)
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
		o     []byte
		input RespArray
	}

	tc := []input{
		{
			o: []byte("*3\r\n$3\r\nfoo\r\n+bar\r\n:-1\r\n"),
			input: RespArray{
				A: []RespMarshaler{
					BulkString{[]byte("foo"), false},
					SimpleString{"bar"},
					SimpleInt{-1},
				},
			},
		},
	}

	for _, test := range tc {
		var b bytes.Buffer
		err := test.input.MarshalRESP(&b)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if b.String() != string(test.o) {
			t.Errorf("input %q, got %q", test.o, b.String())
		}
	}
}

func TestRespArray_UnmarshalRESP(t *testing.T) {
	type input struct {
		i        []byte
		expected RespArray
	}

	tc := []input{
		{
			i: []byte("*3\r\n$3\r\nfoo\r\n+bar\r\n:-1\r\n"),
			expected: RespArray{
				A: []RespMarshaler{
					BulkString{[]byte("foo"), false},
					SimpleString{"bar"},
					SimpleInt{-1},
				},
			},
		},
	}

	for _, test := range tc {
		var s RespArray
		r := bufio.NewReader(bytes.NewReader(test.i))
		err := s.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if !AssertRespArray(s, test.expected) {
			t.Errorf("input %v, got %v", test.expected, s)
		}
	}
}

func AssertRespArray(a RespArray, b RespArray) bool {
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
		expected RespMarshaler
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
			expected: RespArray{
				A: []RespMarshaler{
					BulkString{[]byte("foo"), false},
					SimpleString{"bar"},
					SimpleInt{-1},
				},
			},
		},
	}
	for _, test := range tests {
		var s AnyResp
		r := bufio.NewReader(bytes.NewReader(test.input))
		err := s.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if !AssertAny(s, test.expected) {
			t.Errorf("input %v, got %v", test.expected, s)
		}
	}
}

func TestAnyResp_MarshalRESP(t *testing.T) {
	type test struct {
		input    AnyResp
		expected string
	}
	tests := []test{
		{
			input:    AnyResp{"simple", false},
			expected: "+simple\r\n",
		},
		{
			input:    AnyResp{"", false},
			expected: "+\r\n",
		},
		{
			input:    AnyResp{errors.New("RESP error"), false},
			expected: "-RESP error\r\n",
		},
		{
			input:    AnyResp{nil, true},
			expected: "$-1\r\n",
		},
		{
			input:    AnyResp{[]byte("hello"), true},
			expected: "$5\r\nhello\r\n",
		},
	}

	for _, tc := range tests {
		var b bytes.Buffer
		err := tc.input.MarshalRESP(&b)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if !CompareTwoByteSlices(b.Bytes(), []byte(tc.expected)) {
			t.Errorf("input %q, got %q", tc.expected, b.String())
		}
	}
}

func AssertAny(a AnyResp, b RespMarshaler) bool {
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
		err := AnyResp{I: test.i}.MarshalRESP(&b)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if b.String() != test.o {
			t.Errorf("input %q, got %q", test.o, b.String())
		}
	}
}

func TestAnyResp_UnmarshalRESPAndMarshalRESP(t *testing.T) {
	type tests struct {
		input  []byte
		output AnyResp
	}
	testCases := []tests{
		{
			input:  []byte("+hello\r\n"),
			output: AnyResp{SimpleString{"hello"}, false},
		},
		{
			input:  []byte("-error\r\n"),
			output: AnyResp{SimpleError{"error"}, false},
		},
		{
			input:  []byte(":123\r\n"),
			output: AnyResp{SimpleInt{123}, false},
		},
		{
			input:  []byte("$5\r\nhello\r\n"),
			output: AnyResp{BulkString{[]byte("hello"), false}, false},
		},
		{
			input: []byte("*3\r\n$3\r\nfoo\r\n+bar\r\n:-1\r\n"),
			output: AnyResp{RespArray{
				A: []RespMarshaler{
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
		var marshaled AnyResp
		marshaled.I = test.output.I
		buff := bytes.NewBuffer([]byte{})
		err := marshaled.MarshalRESP(buff)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if buff.String() != string(test.input) {
			t.Errorf("input %q, got %q", test.input, buff.String())
		}

		tUnmarshaler := reflect.TypeOf(test.output)
		unmarshaled, ok := reflect.New(tUnmarshaler).Interface().(RespUnmarshaler)
		if !ok {
			t.Errorf("unexpected error: %s", err)
		}
		r := bufio.NewReader(bytes.NewReader(test.input))
		err = unmarshaled.UnmarshalRESP(r)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		val := reflect.ValueOf(unmarshaled)
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}
		if AssertAny(val.Interface().(AnyResp), test.output) {
			t.Errorf("input %v, got %q", test.output, val)
		}

	}
}
