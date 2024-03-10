package middleware

import (
	"bytes"
	"context"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/handlers"
	"github.com/codecrafters-io/redis-starter-go/app/lib/repl"
)

type HandleStringsRepl struct {
	replicas []*repl.Replica
	next     *handlers.StringsSetHandler
}

func NewReplSet(next *handlers.StringsSetHandler, replica []*repl.Replica) *HandleStringsRepl {
	return &HandleStringsRepl{next: next, replicas: replica}
}

func (h HandleStringsRepl) HandleResp(ctx context.Context, args *resp.Array) (interface{}, error) {
	buff := bytes.NewBuffer(make([]byte, 0, 1024))
	arr := resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("SET")}}}
	arr.AppendArray(args)
	arr.MarshalRESP(buff)
	for _, r := range h.replicas {
		r.Send(buff.Bytes())
	}

	return h.next.HandleResp(ctx, args)
}
