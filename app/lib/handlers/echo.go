package handlers

import (
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
)

func Echo(ctx context.Context, args *resp.AnyResp) (interface{}, error) {
	if args == nil {
		return fmt.Errorf("ERR wrong number of arguments for command"), nil
	}
	return args, nil
}
