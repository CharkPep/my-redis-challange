package lib

import (
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
)

func HandleConfig(ctx context.Context, req *RESPRequest) (interface{}, error) {
	if len(req.Args.A) < 2 {
		return nil, fmt.Errorf("ERR wrong number of arguments")
	}

	var (
		command resp.BulkString
		ok      bool
	)

	if command, ok = req.Args.A[0].(resp.BulkString); !ok {
		return nil, fmt.Errorf("ERR invalid command type")
	}

	switch string(command.S) {
	case "set":
		return nil, nil
	case "get", "GET":
		var key resp.BulkString
		if key, ok = req.Args.A[1].(resp.BulkString); !ok {
			return nil, fmt.Errorf("ERR invalid key type")
		}

		switch string(key.S) {
		case "dir":
			return resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("dir")}, resp.BulkString{S: []byte(req.s.config.PersistenceConfig.Dir)}}}, nil
		case "dbfilename":
			return resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("dbfilename")}, resp.BulkString{S: []byte(req.s.config.PersistenceConfig.File)}}}, nil
		default:
			return nil, fmt.Errorf("ERR invalid key")
		}
	default:
		return nil, fmt.Errorf("ERR invalid command")
	}
}
