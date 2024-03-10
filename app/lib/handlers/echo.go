package handlers

import (
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
)

type EchoHandler struct{}

func (EchoHandler) HandleResp(ctx context.Context, args *resp.Array) (interface{}, error) {
	if len(args.A) < 1 || args == nil {
		return nil, fmt.Errorf("ERR wrong number of arguments for command")
	}

	return args.A[0], nil
}
