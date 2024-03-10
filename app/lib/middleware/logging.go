package middleware

import (
	"context"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"log"
	"time"
)

type Logger struct {
	next lib.HandleRESP
}

func (l Logger) HandleResp(ctx context.Context, args *resp.Array) (interface{}, error) {
	startTime := time.Now()
	res, err := l.next.HandleResp(ctx, args)
	if err != nil {
		log.Println(err)
	}
	log.Printf("Request took %s", time.Since(startTime))
	return res, err
}
