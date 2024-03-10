package middleware

import (
	"bytes"
	"context"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/repl"
)

type ReplWrapper struct {
	replicas *repl.ReplicaManager
	next     lib.HandleRESP
}

func NewReplicationWrapper(next lib.HandleRESP, replica *repl.ReplicaManager) *ReplWrapper {
	return &ReplWrapper{next: next, replicas: replica}
}

func (h ReplWrapper) HandleResp(ctx context.Context, args *resp.Array) (interface{}, error) {
	buff := bytes.NewBuffer(make([]byte, 0, 1024))
	arr := resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("SET")}}}
	arr.AppendArray(args)
	arr.MarshalRESP(buff)
	h.replicas.PropagateToAll(buff.Bytes())

	return h.next.HandleResp(ctx, args)
}
