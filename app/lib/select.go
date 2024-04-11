package lib

import (
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
)

func HandleSelect(ctx context.Context, req *RESPRequest) (interface{}, error) {
	if len(req.Args.A) < 1 {
		return nil, fmt.Errorf("wrong number of arguments for select command")
	}

	db, ok := req.Args.A[0].(resp.SimpleInt)
	if !ok {
		return nil, fmt.Errorf("wrong type of index argument, expected %T, got %T", resp.SimpleInt{}, req.Args.A[0])
	}

	if err := req.SetDb(int(db.I)); err != nil {
		return nil, err
	}

	return "OK", nil
}
