package handlers

import (
	"context"
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/storage"
	"regexp"
	"strconv"
	"time"
)

type SetArgs struct {
	Key    string
	Value  string
	Expire time.Time
	NX     bool
	XX     bool
	GET    bool
}

func parseSetArgs(args *[]resp.Marshaller) (*SetArgs, error) {
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
					setArgs.Expire = expireTime
				case resp.BulkString:
					i++
					parsedTime, err := strconv.ParseInt(string(v.S), 10, 64)
					if err != nil {
						return nil, errors.New("ERR invalid expire time")
					}
					expireTime := time.Now().Add(time.Duration(parsedTime) * time.Second)
					setArgs.Expire = expireTime
				default:
					return nil, errors.New("ERR invalid expire time")
				}
			case "PX", "px":
				if i+1 >= len(*args) {
					return nil, errors.New("ERR wrong number of arguments")
				}
				switch v := (*args)[i+1].(type) {
				case resp.SimpleInt:
					i++
					expireTime := time.Now().Add(time.Duration(v.I) * time.Millisecond)
					setArgs.Expire = expireTime
				case resp.BulkString:
					i++
					parsedTime, err := strconv.ParseInt(string(v.S), 10, 64)
					if err != nil {
						return nil, errors.New("ERR invalid expire time")
					}
					expireTime := time.Now().Add(time.Duration(parsedTime) * time.Millisecond)
					setArgs.Expire = expireTime
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
					setArgs.Expire = expireTime
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
					setArgs.Expire = expireTime
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

func HandleSet(ctx context.Context, req *lib.RESPRequest) (interface{}, error) {
	setArgs, err := parseSetArgs(&req.Args.A)
	if err != nil {
		return nil, err
	}

	strStore := req.Db.GetStorage(storage.STRINGS).(storage.StringsStorage)
	if setArgs.NX {
		// set only if [N]ot e[X]ists
		if _, ok, err := strStore.Get(setArgs.Key); ok || err != nil {
			return nil, err
		}
	}

	if setArgs.XX {
		// set only if [e]xists
		if _, ok, err := strStore.Get(setArgs.Key); !ok || err != nil {
			return nil, err
		}
	}

	//return previous value
	if setArgs.GET {
		oldValue, _, err := strStore.Get(setArgs.Key)
		if err != nil {
			return nil, err
		}

		if err := strStore.Set(setArgs.Key, setArgs.Value, setArgs.Expire); err != nil {
			return nil, err
		}

		return oldValue, err
	}

	if err := strStore.Set(setArgs.Key, setArgs.Value, setArgs.Expire); err != nil {
		return nil, err
	}

	return "OK", err
}

func HandleGet(ctx context.Context, req *lib.RESPRequest) (interface{}, error) {
	if len(req.Args.A) != 1 {
		return nil, fmt.Errorf("ERR wrong number of arguments")
	}

	key, ok := req.Args.A[0].(resp.BulkString)
	if !ok {
		return nil, fmt.Errorf("ERR invalid key type, expected string, got %T", req.Args.A[0])
	}

	db := req.Db.GetStorage(storage.STRINGS).(storage.StringsStorage)
	value, ok, err := db.Get(string(key.S))
	if err != nil {
		return nil, err
	}
	if !ok {
		return resp.BulkString{S: nil, EncodeNil: true}, nil
	}

	return []byte(value), nil
}

func HandleKeys(ctx context.Context, req *lib.RESPRequest) (interface{}, error) {
	if len(req.Args.A) != 1 {
		return nil, fmt.Errorf("ERR wrong number of arguments")
	}

	_, ok := req.Args.A[0].(resp.BulkString)
	if !ok {
		return nil, fmt.Errorf("ERR invalid pattern type, expected string, got %T", req.Args.A[0])
	}

	reg := regexp.MustCompile(".*")
	keys := req.Db.GetStorage(storage.STRINGS).(storage.StringsStorage).Keys(reg)
	array := make([]resp.Marshaller, 0, len(keys))
	for _, key := range keys {
		array = append(array, resp.BulkString{S: []byte(key)})
	}

	return resp.Array{A: array}, nil
}
