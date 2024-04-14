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
	type StreamIdKp struct {
		stream string
		id     string
	}
	if len(req.Args.A) < 2 {
		return nil, fmt.Errorf("wrong number of arguments")
	}

	if streams, ok := req.Args.A[0].(resp.BulkString); !ok || strings.ToLower(streams.String()) != "streams" {
		return nil, fmt.Errorf("wrong arguments")
	}

	var res resp.Array
	var streamKvsInput []StreamIdKp

	for i := 1; i < len(req.Args.A)/2+1; i += 1 {
		if i+1 >= len(req.Args.A) {
			return nil, fmt.Errorf("wrong number of arguments")
		}

		stream, ok := req.Args.A[i].(resp.BulkString)
		if !ok {
			return nil, fmt.Errorf("unexpected type of the key, got %T", req.Args.A[i])
		}

		var start string
		startResp, ok := req.Args.A[len(req.Args.A)/2+i].(resp.BulkString)
		if !ok {
			return nil, fmt.Errorf("unexpected type of the val, got %T", req.Args.A[i+1])
		}

		start = startResp.String()
		if len(strings.Split(start, "-")) == 1 {
			return nil, fmt.Errorf("wrong arguments")
		}

		sequence, err := strconv.ParseInt(strings.Split(start, "-")[1], 10, 64)
		if err != nil {
			return nil, err
		}

		start = fmt.Sprintf("%s-%d", strings.Split(start, "-")[0], sequence+1)
		streamKvsInput = append(streamKvsInput, StreamIdKp{
			stream: stream.String(),
			id:     start,
		})
	}

	for _, stream := range streamKvsInput {
		s, err := req.Db.GetStorage(storage.STREAMS).(*storage.StreamsIdx).GetOrCreateStream(stream.stream)
		if err != nil {
			return nil, err
		}

		kvs := s.Range(stream.id, "")
		streamReadResult := resp.Array{
			A: []resp.Marshaller{
				resp.BulkString{S: []byte(stream.stream)},
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
