package lib

import (
	"bufio"
	"bytes"
	"context"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/persistence"
	"github.com/codecrafters-io/redis-starter-go/app/lib/repl"
	"log"
	"net"
	"os"
	"sync/atomic"
	"testing"
)

func TestPsyncHandler_HandleResp(t *testing.T) {
	type tt struct {
		input  *RESPRequest
		expect *resp.Rdb
		err    error
	}
	tests := []tt{
		{
			input: &RESPRequest{
				Args: &resp.Array{
					A: []resp.Marshaller{
						resp.SimpleString{"psync"},
						resp.BulkString{S: []byte("master")},
						resp.BulkString{S: []byte("0")},
					},
				},
				s: &Server{
					config: &ServerConfig{
						ReplicationConfig: &repl.ReplicationConfig{
							MasterReplid:     "123",
							MasterReplOffset: atomic.Uint64{},
						},
					},
				},
				Logger: log.New(os.Stdout, "test: ", log.LstdFlags),
			},
			err:    nil,
			expect: persistence.GetEmpty(),
		},
	}

	for _, test := range tests {
		h := &PsyncHandler{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		srv, client := net.Pipe()
		test.input.conn = client
		errChan := make(chan error)
		go func(req *RESPRequest) {
			_, err := h.HandleResp(ctx, test.input)
			errChan <- err
		}(test.input)

		fullResync := resp.SimpleString{}
		if _, err := fullResync.UnmarshalRESP(bufio.NewReader(srv)); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		rdb := resp.Rdb{}
		if err := rdb.UnmarshalRESP(bufio.NewReader(srv)); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}

		t.Logf("RDB in bytes: %d", len(rdb.Content))
		if !bytes.Equal(rdb.Content, test.expect.Content) {
			t.Fatalf("expected %q, got %q", test.expect.Content, rdb.Content)
		}
		cancel()
		select {
		case err := <-errChan:
			if err != test.err {
				t.Fatalf("expected %v, got %v", test.err, err)
			}
		}

	}

}
