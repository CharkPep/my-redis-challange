package handlers

import (
	"context"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/storage"
)

func HandleXAdd(ctx context.Context, req *lib.RESPRequest) (interface{}, error) {
	if len(req.Args.A) < 2 {
		return nil, fmt.Errorf("wrong number of arguments")
	}

	stream, ok := req.Args.A[0].(resp.BulkString)
	if !ok {
		return nil, fmt.Errorf("unexpected type of the key, got %T", req.Args.A[0])
	}

	key, ok := req.Args.A[1].(resp.BulkString)
	if !ok {
		return nil, fmt.Errorf("unexpected type of the val, got %T", req.Args.A[1])
	}

	s, err := req.Db.GetStorage(storage.STREAMS).(*storage.StreamsIdx).GetOrCreateStream(stream.String())
	if err != nil {
		return nil, err
	}

	kVals := make(map[string]string)
	for i := 2; i < len(req.Args.A); i += 2 {
		field, ok := req.Args.A[i].(resp.BulkString)
		if !ok {
			return nil, fmt.Errorf("unexpected type of the kv field, got %T", req.Args.A[1])
		}

		if i+1 > len(req.Args.A) {
			return nil, fmt.Errorf("wrong number of arguments")
		}

		value, ok := req.Args.A[i+1].(resp.BulkString)
		if !ok {
			return nil, fmt.Errorf("unexpected type of the kv value, got %T", req.Args.A[1])
		}

		kVals[field.String()] = value.String()
	}

	var k string
	if _, k, _, err = s.Add(key.String(), kVals); err != nil {
		return nil, err
	}

	return []byte(k), nil
}
