package handlers

import (
	"context"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
)

type PingHandler struct{}

func (PingHandler) HandleResp(ctx context.Context, req *lib.RESPRequest) (interface{}, error) {
	return "PONG", nil
}
