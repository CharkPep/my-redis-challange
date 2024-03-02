package handlers

import (
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"testing"
	"time"
)

func TestParseSetArgs(t *testing.T) {
	type testCase struct {
		input     []resp.RespMarshaler
		expected  *SetArgs
		isToThrow bool
		error     string
	}

	tests := []testCase{
		{
			input: []resp.RespMarshaler{resp.SimpleString{"key"}, resp.SimpleString{"value"}},
			expected: &SetArgs{
				Key:   "key",
				Value: "value",
			},
			error:     "",
			isToThrow: false,
		},
		{
			input: []resp.RespMarshaler{
				resp.BulkString{S: []byte("key")},
				resp.BulkString{S: []byte("value")},
				resp.BulkString{S: []byte("NX")}},
			expected: &SetArgs{
				NX:    true,
				Value: "value",
				Key:   "key",
			},
		},
		{
			input: []resp.RespMarshaler{
				resp.BulkString{S: []byte("key")},
				resp.BulkString{S: []byte("value")},
				resp.BulkString{S: []byte("XX")},
			},
			expected: &SetArgs{
				Key:   "key",
				Value: "value",
				XX:    true,
			},
		},
		{
			input: []resp.RespMarshaler{
				resp.BulkString{S: []byte("key")},
				resp.BulkString{S: []byte("value")},
				resp.BulkString{S: []byte("XX")},
				resp.BulkString{S: []byte("NX")},
			},
			expected:  nil,
			error:     "ERR invalid argument, XX and NX are mutually exclusive",
			isToThrow: true,
		},
	}

	for _, test := range tests {
		result, err := parseSetArgs(&test.input)
		if test.isToThrow && err.Error() != test.error {
			t.Errorf("expected %v, got %v", test.error, err.Error())
			continue
		}

		if !test.isToThrow && err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		if !test.isToThrow && *result != *test.expected {
			t.Errorf("expected %v, got %v", test.expected, result)
		}

	}
}

func TestParseSetArgsExpire(t *testing.T) {
	type testCase struct {
		input    []resp.RespMarshaler
		expected *SetArgs
		expiry   time.Duration
	}

	tests := []testCase{
		{
			input: []resp.RespMarshaler{
				resp.BulkString{S: []byte("key")},
				resp.BulkString{S: []byte("value")},
				resp.BulkString{S: []byte("EX")},
				resp.SimpleInt{I: 10},
			},
			expected: &SetArgs{
				Key:    "key",
				Value:  "value",
				Expire: &time.Time{},
			},
			expiry: time.Second * 10,
		},
		{
			input: []resp.RespMarshaler{
				resp.BulkString{S: []byte("key")},
				resp.BulkString{S: []byte("value")},
				resp.BulkString{S: []byte("PX")},
				resp.SimpleInt{I: 10},
			},
			expected: &SetArgs{
				Key:    "key",
				Value:  "value",
				Expire: &time.Time{},
			},
			expiry: time.Millisecond * 10,
		},
		{
			input: []resp.RespMarshaler{
				resp.BulkString{S: []byte("key")},
				resp.BulkString{S: []byte("value")},
				resp.BulkString{S: []byte("EXAT")},
				resp.SimpleInt{I: time.Now().Add(time.Second * 10).Unix()},
			},
			expected: &SetArgs{
				Key:    "key",
				Value:  "value",
				Expire: &time.Time{},
			},
			expiry: time.Second * 10,
		},
		{
			input: []resp.RespMarshaler{
				resp.BulkString{S: []byte("key")},
				resp.BulkString{S: []byte("value")},
				resp.BulkString{S: []byte("PXAT")},
				resp.SimpleInt{I: (time.Now().Add(time.Second * 10).UnixMilli())},
			},
			expected: &SetArgs{
				Key:    "key",
				Value:  "value",
				Expire: &time.Time{},
			},
			expiry: time.Second * 10,
		},
	}

	EPS := 1 * time.Second
	for i, test := range tests {
		startTime := time.Now()
		result, err := parseSetArgs(&test.input)
		if err != nil {
			t.Errorf("unexpected error: %s, case %d", err, i)
			continue
		}

		if result.Expire == nil {
			t.Errorf("expected %v, got %v", test.expected, result)
			continue
		}

		if result.Expire.Sub(startTime.Add(test.expiry)) > EPS || result.Expire.Sub(startTime.Add(test.expiry)) < -EPS {
			t.Errorf("Expire in the test %d", test.input[3].(resp.SimpleInt).I)
			t.Errorf("Expire time is not correct, expected %v, got from the test start %v, case %d", test.expiry, result.Expire.Sub(startTime), i)
		}
	}
}
