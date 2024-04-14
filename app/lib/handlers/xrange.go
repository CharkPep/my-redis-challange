package handlers

import (
	"context"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/storage"
)

func HandleXRange(ctx context.Context, req *lib.RESPRequest) (interface{}, error) {
	if len(req.Args.A) > 3 {
		return nil, fmt.Errorf("wrong number of arguments")
	}

	stream, ok := req.Args.A[0].(resp.BulkString)
	if !ok {
		return nil, fmt.Errorf("unexpected type of the key, got %T", req.Args.A[0])
	}

	s, err := req.Db.GetStorage(storage.STREAMS).(*storage.StreamsIdx).GetOrCreateStream(stream.String())
	if err != nil {
		return nil, err
	}

	var start, end string
	if len(req.Args.A) == 0 {
		start = "0"
		end, _ = s.Min("")
	} else if len(req.Args.A) == 3 {
		startResp, ok := req.Args.A[1].(resp.BulkString)
		if !ok {
			return nil, fmt.Errorf("unexpected type of the val, got %T", req.Args.A[1])
		}

		endResp, ok := req.Args.A[2].(resp.BulkString)
		if !ok {
			return nil, fmt.Errorf("unexpected type of the val, got %T", req.Args.A[1])
		}

		start = startResp.String()
		end = endResp.String()
	} else {
		return nil, fmt.Errorf("wrong number of arguments")
	}

	if start == "-" {
		start, _ = s.Min("")
	}

	if end == "+" {
		end, _ = s.Max("")
	}

	req.Logger.Printf("Start %s, End %s", start, end)
	kvs := s.Range(start, end)
	var res resp.Array
	for _, k := range kvs {
		var inner resp.Array
		inner.A = append(inner.A, resp.BulkString{S: []byte(k.Key)})
		data := resp.Array{}
		for _, v := range k.Data {
			data.A = append(data.A, resp.BulkString{S: []byte(v)})
		}

		inner.A = append(inner.A, data)
		res.A = append(res.A, inner)
	}

	return res, nil

}
