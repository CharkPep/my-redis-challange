package lib

import (
	"bytes"
	"context"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
)

type ReplWrapper struct {
	Next Handler
}

func (h ReplWrapper) HandleResp(ctx context.Context, req *RESPRequest) (interface{}, error) {
	args := resp.Array{A: make([]resp.Marshaller, len(req.Args.A))}
	copy(args.A, req.Args.A)
	// Need to check if write was successful before propagating
	res, err := h.Next.HandleResp(ctx, req)
	if err != nil {
		return nil, err
	}
	buff := bytes.NewBuffer(make([]byte, 0, 1024))
	arr := resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("SET")}}}
	arr.AppendArray(&args)
	arr.MarshalRESP(buff)
	req.Logger.Printf("propagation %q", buff)
	req.s.PropagateToAll(buff.Bytes())
	if !req.Propagation {
		req.s.config.ReplicationConfig.MasterReplOffset.Add(uint64(len(buff.Bytes())))
	}

	return res, nil
}
