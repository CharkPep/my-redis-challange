package handlers

import (
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"reflect"
	"testing"
	"time"
)

func TestParseXReadArgs(t *testing.T) {
	type tt struct {
		i   resp.Array
		e   xReadArgs
		err error
	}

	ts := []tt{
		{
			i: resp.Array{
				[]resp.Marshaller{
					resp.BulkString{S: []byte("block")},
					resp.BulkString{S: []byte("1000")},
					resp.BulkString{S: []byte("streams")},
					resp.BulkString{S: []byte("stream")},
					resp.BulkString{S: []byte("0-1")},
				},
			},
			e: xReadArgs{
				streams: []Stream{
					{
						stream: "stream",
						start:  "0-1",
					},
				},
				block: time.Millisecond * 1000,
				count: 1,
			},
		},
	}

	for _, test := range ts {
		res, err := parseXReadArgs(&test.i)
		if err != nil && test.err == nil {
			t.Error(err)
		}

		if res != nil {
			if !reflect.DeepEqual(*res, test.e) {
				t.Errorf("expected %v, got %v", test.e, res)
			}
		}
	}
}
