package handlers

import (
	"context"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	"github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
)

func HandleType(ctx context.Context, req *lib.RESPRequest) (interface{}, error) {
	if len(req.Args.A) < 1 {
		return nil, fmt.Errorf("wrong number of arguments")
	}

	key, ok := req.Args.A[0].(encoding.BulkString)
	if !ok {
		return nil, fmt.Errorf("wrong argument type")
	}

	t := req.Db.GetType(key.String())
	return t.String(), nil
}
