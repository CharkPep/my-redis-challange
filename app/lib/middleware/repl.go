package middleware

import (
	"bytes"
	"context"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
)

type ReplWrapper struct {
	next lib.HandleRESP
}

func NewReplicationWrapper(next lib.HandleRESP) *ReplWrapper {
	return &ReplWrapper{next: next}
}

func (h ReplWrapper) HandleResp(ctx context.Context, req *lib.RESPRequest) (interface{}, error) {
	// Need to check if write was successful before propagating
	res, err := h.next.HandleResp(ctx, req)
	if err != nil {
		return nil, err
	}
	buff := bytes.NewBuffer(make([]byte, 0, 1024))
	arr := resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("SET")}}}
	arr.AppendArray(req.Args)
	arr.MarshalRESP(buff)
	req.S.PropagateToAll(buff.Bytes())
	return res, nil
}
