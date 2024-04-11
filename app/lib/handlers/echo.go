package handlers

import (
	"context"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
)

func HandleEcho(ctx context.Context, req *lib.RESPRequest) (interface{}, error) {
	if req.Args == nil || len(req.Args.A) < 1 {
		return nil, fmt.Errorf("ERR wrong number of arguments for command")
	}

	return req.Args.A[0], nil
}
