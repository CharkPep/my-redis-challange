package lib

import (
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
)

type Router struct {
	handlers map[string]HandleRESP
}

func NewRouter() *Router {
	return &Router{
		handlers: make(map[string]HandleRESP),
	}
}

func (r *Router) RegisterHandler(path string, handler HandleRESP) {
	r.handlers[path] = handler
}

func (r *Router) ResolveRequest(args *resp.Array) (HandleRESP, error) {
	command, err := r.getCommand(&args.A)
	if err != nil {
		return nil, err
	}

	handler, ok := r.handlers[command]
	if !ok {
		return nil, fmt.Errorf("unknown command: %s", command)
	}

	return handler, nil
}

func (r *Router) getCommand(args *[]resp.Marshaller) (string, error) {
	if len(*args) == 0 {
		return "", fmt.Errorf("empty command")
	}

	switch command := (*args)[0].(type) {
	case resp.SimpleString:
		return command.S, nil
	case resp.BulkString:
		return string(command.S), nil
	}

	return "", fmt.Errorf("invalid command type: %T", (*args)[0])
}
