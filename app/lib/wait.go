package lib

import (
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/replication"
	"strconv"
	"time"
)

func HandleWait(ctx context.Context, req *RESPRequest) (interface{}, error) {
	var (

		// Wait for n slaves
		n int64

		// In-sync
		N int64

		// Timeout
		timeout time.Duration
	)
	if len(req.Args.A) < 2 {
		return nil, fmt.Errorf("ERR wrong number of arguments")
	}

	if v, ok := req.Args.A[0].(resp.BulkString); ok {
		if v.S == nil {
			return nil, fmt.Errorf("ERR invalid number of slaves")
		}

		i, err := strconv.ParseInt(string(v.S), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("ERR invalid number of slaves")
		}
		n = i
	}

	if v, ok := req.Args.A[1].(resp.BulkString); ok {
		if v.S == nil {
			return nil, fmt.Errorf("ERR invalid timeout")
		}

		i, err := strconv.ParseInt(string(v.S), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("ERR invalid timeout")
		}
		timeout = time.Duration(i) * time.Millisecond
	}

	acks := make(chan uint64)
	req.Logger.Printf("Target offset: %d", req.s.config.ReplicationConfig.MasterReplOffset.Load())
	for _, r := range req.s.slaves {
		if r.GetOffset() >= req.s.config.ReplicationConfig.MasterReplOffset.Load() {
			N++
			// First of all why you read it?, second the tester on codecrafters is responding with offset that is a sum of
			// prev_master_offset + len(set_message) + len(ack_message), why? idk, but as i cached responses to reduce round trips overhead
			// i had to repeat GetAck for each replication, otherwise the tester will fail(((
			go func(r *replication.Slave) {
				r.GetAck(timeout)
			}(r)
			continue
		}

		go func(r *replication.Slave) {
			offset, err := r.GetAck(timeout)
			if err != nil {
				req.Logger.Printf("Error getting ack: %s", err)
			}

			acks <- offset
		}(r)
	}

	req.Logger.Printf("Waiting for %d slaves, total %d of %d, timeout %s", n, N, len(req.s.slaves), timeout)
	for i := N; i < n; i++ {
		select {
		case offset := <-acks:
			if offset >= req.s.config.ReplicationConfig.MasterReplOffset.Load() {
				N++
			}

			if N >= n {
				break
			}
		case <-time.After(timeout):
			req.Logger.Printf("timeout excited")
			break
		}
	}

	req.Logger.Printf("Returning %d", N)
	return N, nil
}
