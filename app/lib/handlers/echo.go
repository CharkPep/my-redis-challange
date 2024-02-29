package handlers

import (
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
)

func RemoveCommand(expression resp.AnyResp) (*resp.AnyResp, error) {
	switch expression.I.(type) {
	case resp.SimpleString:
		return nil, nil
	case resp.BulkString:
		return nil, nil
	case resp.RespArray:
		elem := expression.I.(resp.RespArray).A[1:]
		return &resp.AnyResp{I: elem}, nil
	}
	return nil, fmt.Errorf("invalid command type: %T", expression.I)
}

func Echo(ctx context.Context, args *resp.AnyResp) (interface{}, error) {
	return RemoveCommand(*args)
}
