package lib

import (
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"strings"
)

type Router struct {
	handlers map[string]Handler
}

func NewRouter() *Router {
	return &Router{
		handlers: make(map[string]Handler),
	}
}

func (r *Router) RegisterHandlerFunc(path string, handler func(ctx context.Context, req *RESPRequest) (interface{}, error)) {
	r.handlers[path] = HandleFunc(handler)
}

func (r *Router) RegisterHandler(path string, handler Handler) {
	r.handlers[path] = handler
}

func (r *Router) ResolveRequest(args *resp.Array) (Handler, error) {
	command, err := r.getCommand(&args.A)
	if err != nil {
		return nil, err
	}

	handler, ok := r.handlers[strings.ToLower(command)]
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
		return strings.ToLower(command.S), nil
	case resp.BulkString:
		return strings.ToLower(string(command.S)), nil
	}

	return "", fmt.Errorf("invalid command type: %T", (*args)[0])
}
