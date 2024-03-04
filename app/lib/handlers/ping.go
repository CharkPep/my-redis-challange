package handlers

import (
	"context"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
)

func Ping(ctx context.Context, args *resp.Array) (interface{}, error) {
	return "PONG", nil
}
