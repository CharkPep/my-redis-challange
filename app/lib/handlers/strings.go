package handlers

import (
	"context"
	"errors"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/storage"
	"strconv"
	"time"
)

type StringHandler struct {
	Storage *storage.StringsStorage
}

type SetArgs struct {
	Key    string
	Value  string
	Expire *time.Time
	NX     bool
	XX     bool
	GET    bool
}

func parseSetArgs(args *[]resp.RespMarshaler) (*SetArgs, error) {
	if len(*args) < 2 {
		return nil, errors.New("ERR wrong number of arguments")
	}

	var setArgs SetArgs
	switch key := (*args)[0].(type) {
	case resp.SimpleString:
		setArgs.Key = key.S
	case resp.BulkString:
		setArgs.Key = string(key.S)
	default:
		return nil, fmt.Errorf("ERR invalid key type, expected string, got %T", key)
	}

	switch value := (*args)[1].(type) {
	case resp.SimpleString:
		if value.S == "" {
			return nil, errors.New("ERR invalid value")
		}
		setArgs.Value = value.S
	case resp.BulkString:
		if len(value.S) == 0 {
			return nil, errors.New("ERR invalid value")
		}
		setArgs.Value = string(value.S)
	default:
		return nil, fmt.Errorf("ERR invalid value type, expected string, got %T", value)
	}

	for i := 2; i < len(*args); i++ {
		switch arg := (*args)[i].(type) {
		case resp.SimpleString, resp.BulkString:
			val := ""
			switch arg := arg.(type) {
			case resp.SimpleString:
				val = arg.S
			case resp.BulkString:
				val = string(arg.S)
			}
			switch val {
			case "NX", "nx":
				if setArgs.XX {
					return nil, errors.New("ERR invalid argument, XX and NX are mutually exclusive")
				}
				setArgs.NX = true
			case "XX", "xx":
				if setArgs.NX {
					return nil, errors.New("ERR invalid argument, XX and NX are mutually exclusive")
				}
				setArgs.XX = true
			case "EX", "ex":
				if i+1 >= len(*args) {
					return nil, errors.New("ERR wrong number of arguments")
				}
				switch v := (*args)[i+1].(type) {
				case resp.SimpleInt:
					i++
					expireTime := time.Now().Add(time.Duration(v.I) * time.Second)
					setArgs.Expire = &expireTime
				case resp.BulkString:
					i++
					parsedTime, err := strconv.ParseInt(string(v.S), 10, 64)
					if err != nil {
						return nil, errors.New("ERR invalid expire time")
					}
					expireTime := time.Now().Add(time.Duration(parsedTime) * time.Second)
					setArgs.Expire = &expireTime
				default:
					return nil, errors.New("ERR invalid expire time")
				}
			case "PX", "px":
				fmt.Println("GOT NPX RIGHT HERE GOD DAM")
				if i+1 >= len(*args) {
					return nil, errors.New("ERR wrong number of arguments")
				}
				switch v := (*args)[i+1].(type) {
				case resp.SimpleInt:
					i++
					expireTime := time.Now().Add(time.Duration(v.I) * time.Millisecond)
					setArgs.Expire = &expireTime
				case resp.BulkString:
					i++
					parsedTime, err := strconv.ParseInt(string(v.S), 10, 64)
					if err != nil {
						return nil, errors.New("ERR invalid expire time")
					}
					expireTime := time.Now().Add(time.Duration(parsedTime) * time.Millisecond)
					setArgs.Expire = &expireTime
				default:
					return nil, errors.New("ERR invalid expire time")
				}
			case "EXAT", "exat":
				if i+1 >= len(*args) {
					return nil, errors.New("ERR wrong number of arguments")
				}
				if expire, ok := ((*args)[i+1]).(resp.SimpleInt); !ok {
					return nil, errors.New("ERR invalid expire time")
				} else {
					i++
					expireTime := time.Unix(expire.I, 0)
					setArgs.Expire = &expireTime
				}
			case "PXAT", "pxat":
				if i+1 >= len(*args) {
					return nil, errors.New("ERR wrong number of arguments")
				}
				if expire, ok := ((*args)[i+1]).(resp.SimpleInt); !ok {
					return nil, errors.New("ERR invalid expire time")
				} else {
					i++
					expireTime := time.UnixMilli(expire.I)
					setArgs.Expire = &expireTime
				}
			case "GET", "get":
				setArgs.GET = true
			default:
				return nil, fmt.Errorf("ERR invalid argument: %s", arg)
			}

		default:
			return nil, fmt.Errorf("ERR invalid argument type, expected string or int, got %T", arg)
		}
	}

	return &setArgs, nil
}

func (sh StringHandler) HandleSet(ctx context.Context, args *resp.RespArray) (interface{}, error) {
	setArgs, err := parseSetArgs(&args.A)
	if err != nil {
		return nil, err
	}
	if setArgs.NX {
		if _, ok := sh.Storage.Get(setArgs.Key); ok {
			return nil, nil
		}

	}

	if setArgs.XX {
		if _, ok := sh.Storage.Get(setArgs.Key); !ok {
			return nil, nil
		}
	}

	if setArgs.GET {
		oldValue, _ := sh.Storage.Get(setArgs.Key)
		sh.Storage.Set(setArgs.Key, setArgs.Value, setArgs.Expire)
		return oldValue, err
	}

	sh.Storage.Set(setArgs.Key, setArgs.Value, setArgs.Expire)
	return "OK", err
}

func (sh StringHandler) HandleGet(ctx context.Context, args *resp.RespArray) (interface{}, error) {
	if len(args.A) != 1 {
		return nil, fmt.Errorf("ERR wrong number of arguments")
	}
	key, ok := args.A[0].(resp.BulkString)
	if !ok {
		return nil, fmt.Errorf("ERR invalid key type, expected string, got %T", args.A[0])
	}
	value, ok := sh.Storage.Get(string(key.S))
	if !ok {
		return resp.BulkString{S: nil, EncodeNil: true}, nil
	}
	return value, nil
}
