package handlers

import (
	"context"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/storage"
	"strconv"
	"strings"
	"time"
)

type Stream struct {
	stream string
	start  string
}

type xReadArgs struct {
	streams []Stream
	block   time.Duration
	count   int
}

func parseXReadArgs(args *resp.Array) (*xReadArgs, error) {
	readArgs := xReadArgs{
		block: -1,
		count: 1,
	}

	streams := -1
	for i := 0; i < len(args.A); i++ {
		arg, ok := args.A[i].(resp.BulkString)
		if !ok {
			return nil, fmt.Errorf("wrong type of the arguments")
		}

		switch arg.String() {
		case "BLOCK", "block":
			i++
			timeResp, ok := args.A[i].(resp.BulkString)
			if !ok {
				return nil, fmt.Errorf("wrong type of the arguments")
			}

			t, err := strconv.ParseInt(timeResp.String(), 10, 64)
			if err != nil {
				return nil, err
			}

			if t < 0 {
				return nil, fmt.Errorf("wrong arguments")
			}

			readArgs.block = time.Millisecond * time.Duration(t)
			continue
		case "COUNT", "count":
			i++
			countResp, ok := args.A[i].(resp.BulkString)
			if !ok {
				return nil, fmt.Errorf("wrong type of the arguments")
			}

			count, err := strconv.ParseInt(countResp.String(), 10, 64)
			if err != nil {
				return nil, err
			}

			if count < 0 {
				return nil, fmt.Errorf("wrong arguments")
			}

			readArgs.count = int(count)
			continue
		case "STREAMS", "streams":
			streams = i
			continue
		default:
			if streams == -1 {
				return nil, fmt.Errorf("unkown flag %s", arg.String())
			}
		}

		if i == streams+(len(args.A)-streams)/2+1 {
			break
		}

		//if streams+(len(args.A)-streams)/2 > len(args.A) {
		//	fmt.Printf("Idx %d\n", i)
		//	return nil, fmt.Errorf("wrong number of arguments")
		//}

		startResp, ok := args.A[i+(len(args.A)-streams)/2].(resp.BulkString)
		if !ok {
			return nil, fmt.Errorf("unexpected string type of the val, got %T", args.A[i+1])
		}

		if len(strings.Split(startResp.String(), "-")) == 1 {
			return nil, fmt.Errorf("wrong arguments")
		}

		sequence, err := strconv.ParseInt(strings.Split(startResp.String(), "-")[1], 10, 64)
		if err != nil {
			return nil, err
		}

		readArgs.streams = append(readArgs.streams, Stream{
			stream: arg.String(),
			start:  fmt.Sprintf("%s-%d", strings.Split(startResp.String(), "-")[0], sequence+1),
		})

	}

	return &readArgs, nil
}

func HandleXRead(ctx context.Context, req *lib.RESPRequest) (interface{}, error) {
	args, err := parseXReadArgs(req.Args)
	if err != nil {
		return nil, err
	}

	res := resp.Array{}
	if args.block == 0 {
		// connection io timeout, though should remove it
		args.block = time.Second * 10
	}

	timeout := time.After(args.block)
	for _, stream := range args.streams {
		s, err := req.Db.GetStorage(storage.STREAMS).(*storage.StreamsIdx).GetOrCreateStream(stream.stream)
		if err != nil {
			return nil, err
		}

		kvs := s.Range(stream.start, "")
		streamReadResult := resp.Array{
			A: []resp.Marshaller{
				resp.BulkString{S: []byte(stream.stream)},
			},
		}

		req.Logger.Printf("Block set for %s", args.block)
		if len(kvs) == 0 && args.block != -1 {
			req.Logger.Printf("Blocking for %s", args.block)
			id, ch := s.Subscribe()
			done := make(chan struct{})
			read := make(chan storage.StreamKV)
			go func() {
				defer func() {
					s.Unsubscribe(id)
				}()

				for {
					select {
					case kv := <-ch:
						if strings.Compare(kv.Key, stream.start) == 1 || strings.Compare(kv.Key, stream.start) == 0 {
							req.Logger.Printf("%s > %s", kv.Key, stream.stream)
							read <- kv
							continue
						}

					case <-done:
						return
					}
				}
			}()

			select {
			case <-timeout:
				return resp.BulkString{S: nil, EncodeNil: true}, nil
			case kv := <-read:
				done <- struct{}{}
				kvs = append(kvs, kv)
				break
			}
		}

		req.Logger.Printf("Key values %s", kvs)
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
