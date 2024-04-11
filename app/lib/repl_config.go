package lib

// repl_config.go includes server level handlers

import (
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/replication"
	"log"
)

func HandleReplicationConf(ctx context.Context, req *RESPRequest) (interface{}, error) {
	switch v := req.Args.A[0].(type) {
	case resp.BulkString:
		switch string(v.S) {
		case "listening-port", "LISTENING-PORT":
			if len(req.Args.A) < 2 {
				return nil, fmt.Errorf("ERR wrong number of arguments for command")
			}
			log.Printf("Adding replica on port %s", req.Args.A[1].(resp.BulkString).S)
			req.s.slaves = append(req.s.slaves, replication.NewReplica(req.conn, fmt.Sprint(len(req.s.slaves))))
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
