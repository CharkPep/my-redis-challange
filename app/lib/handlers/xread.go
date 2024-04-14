package handlers

import (
	"context"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/storage"
	"strconv"
	"strings"
)

func HandleXRead(ctx context.Context, req *lib.RESPRequest) (interface{}, error) {
	if len(req.Args.A) < 2 {
		return nil, fmt.Errorf("wrong number of arguments")
	}

	if streams, ok := req.Args.A[0].(resp.BulkString); !ok || strings.ToLower(streams.String()) != "streams" {
		return nil, fmt.Errorf("wrong arguments")
	}

	var res resp.Array
	for i := 1; i < len(req.Args.A); i += 2 {
		if i+1 >= len(req.Args.A) {
			return nil, fmt.Errorf("wrong number of arguments")
		}

		stream, ok := req.Args.A[i].(resp.BulkString)
		if !ok {
			return nil, fmt.Errorf("unexpected type of the key, got %T", req.Args.A[0])
		}

		s, err := req.Db.GetStorage(storage.STREAMS).(*storage.StreamsIdx).GetOrCreateStream(stream.String())
		if err != nil {
			return nil, err
		}

		var start string
		startResp, ok := req.Args.A[i+1].(resp.BulkString)
		if !ok {
			return nil, fmt.Errorf("unexpected type of the val, got %T", req.Args.A[1])
		}

		start = startResp.String()
		sequence, err := strconv.ParseInt(strings.Split(start, "-")[1], 10, 64)
		start = fmt.Sprintf("%s-%d", strings.Split(start, "-")[0], sequence+1)
		kvs := s.Range(start, "")
		streamReadResult := resp.Array{
			A: []resp.Marshaller{
				stream,
			},
		}

		kvData := resp.Array{}
		for _, k := range kvs {
			key := resp.Array{
				A: []resp.Marshaller{
					resp.BulkString{S: []byte(k.Key)},
				},
			}
			data := resp.Array{}
			for _, v := range k.Data {
				data.A = append(data.A, resp.BulkString{S: []byte(v)})
			}

			key.A = append(key.A, data)
			kvData.A = append(kvData.A, key)
		}

		streamReadResult.A = append(streamReadResult.A, kvData)
		if len(kvs) != 0 {
			res.A = append(res.A, streamReadResult)
		}
	}

	return res, nil
}
