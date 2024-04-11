package handlers

import (
	"context"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
)

func HandleInfo(ctx context.Context, req *lib.RESPRequest) (interface{}, error) {
	if len(req.Args.A) < 1 {
		return nil, fmt.Errorf("ERR wrong number of arguments")
	}

	section, ok := req.Args.A[0].(resp.BulkString)
	if !ok {
		return nil, fmt.Errorf("ERR invalid section type, expected string, got %T", (req.Args.A)[0])
	}
	switch string(section.S) {
	case "replication":
		return req.Config.ReplicationConfig, nil
	default:
		return nil, fmt.Errorf("ERR invalid section: %s", section.S)
	}
}
