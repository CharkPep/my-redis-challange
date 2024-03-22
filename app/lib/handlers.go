package lib

// handlers.go includes server level handlers

import (
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/persistence"
	"github.com/codecrafters-io/redis-starter-go/app/lib/repl"
	"log"
	"strconv"
	"time"
)

type InfoHandler struct{}

func (i InfoHandler) HandleResp(ctx context.Context, req *RESPRequest) (interface{}, error) {
	if len(req.Args.A) < 1 {
		return nil, fmt.Errorf("ERR wrong number of arguments")
	}
	section, ok := req.Args.A[0].(resp.BulkString)
	if !ok {
		return nil, fmt.Errorf("ERR invalid section type, expected string, got %T", (req.Args.A)[0])
	}
	switch string(section.S) {
	case "replication":
		return req.s.config.ReplicationConfig, nil
	default:
		return nil, fmt.Errorf("ERR invalid section: %s", section.S)
	}
}

type ReplConfHandler struct{}

func (c ReplConfHandler) HandleResp(ctx context.Context, req *RESPRequest) (interface{}, error) {
	switch v := req.Args.A[0].(type) {
	case resp.BulkString:
		switch string(v.S) {
		case "listening-port", "LISTENING-PORT":
			if len(req.Args.A) < 2 {
				return nil, fmt.Errorf("ERR wrong number of arguments for command")
			}
			log.Printf("Adding replica on port %s", req.Args.A[1].(resp.BulkString).S)
			req.s.slaves = append(req.s.slaves, repl.NewReplica(req.conn, fmt.Sprint(len(req.s.slaves))))
			return "OK", nil
		case "capa", "CAPA":
			return "OK", nil
		case "getack", "GETACK":
			(resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("REPLCONF")}, resp.BulkString{S: []byte("ACK")}, resp.BulkString{S: []byte(fmt.Sprint(req.s.config.ReplicationConfig.MasterReplOffset.Load()))}}}).MarshalRESP(req.W)
			return nil, nil
		}
	}
	return nil, fmt.Errorf("ERR invalid command")
}

type PsyncHandler struct {
}

func (p PsyncHandler) HandleResp(ctx context.Context, req *RESPRequest) (interface{}, error) {
	req.Logger.Printf("Sending resync to %s", req.RAddr)
	if _, err := (resp.SimpleString{S: fmt.Sprintf("FULLRESYNC %s %d", req.s.config.ReplicationConfig.MasterReplid,
		req.s.config.ReplicationConfig.MasterReplOffset.Load())}).MarshalRESP(req.W); err != nil {
		return nil, err
	}
	req.Logger.Printf("RDB length %d", len(persistence.GetEmpty().Content))
	req.Logger.Printf("Sending full resync to %s", req.RAddr)
	if err := persistence.GetEmpty().MarshalRESP(req.W); err != nil {
		return nil, err
	}
	if err := req.conn.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}
	if err := req.conn.SetWriteDeadline(time.Time{}); err != nil {
		return nil, err
	}

	req.s.config.ReplicationConfig.ConnectedSlaves.Add(1)
	req.Logger.Printf("Slave connected: %s", req.RAddr)
	<-ctx.Done()
	req.s.config.ReplicationConfig.ConnectedSlaves.Swap(uint64(req.s.config.ReplicationConfig.ConnectedSlaves.Load() - 1))
	return nil, nil
}

type WaitHandler struct{}

func (w WaitHandler) HandleResp(ctx context.Context, req *RESPRequest) (interface{}, error) {
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
			go func(r *repl.Slave) {
				r.GetAck(timeout)
			}(r)
			continue
		}

		go func(r *repl.Slave) {
			offset, err := r.GetAck(timeout)
			if err != nil {
				req.Logger.Printf("Error getting ack: %s", err)
			}
			acks <- offset
		}(r)
	}

	req.Logger.Printf("Waiting for %d slaves, total %d of %d", n, N, len(req.s.slaves))
	for i := N; i < n; i++ {
		select {
		case offset := <-acks:
			if offset > req.s.config.ReplicationConfig.MasterReplOffset.Load() {
				N++
			}

			if N >= n {
				break
			}
		case <-time.After(timeout):
			break
		}
	}

	return N, nil
}
