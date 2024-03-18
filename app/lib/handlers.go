package lib

import (
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/persistence"
	"github.com/codecrafters-io/redis-starter-go/app/lib/repl"
	"log"
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
			req.s.replicas = append(req.s.replicas, repl.NewReplica(req.conn))
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
	req.s.config.ReplicationConfig.ConnectedSlaves += 1
	req.Logger.Printf("Replica connected: %s", req.RAddr)
	<-ctx.Done()
	req.s.config.ReplicationConfig.ConnectedSlaves += -1
	return nil, nil
}
