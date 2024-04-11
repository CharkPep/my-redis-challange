package handlers

import (
	"context"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
)

func HandlePing(ctx context.Context, req *lib.RESPRequest) (interface{}, error) {
	return "PONG", nil
}
