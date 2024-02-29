package lib

import (
	"bufio"
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"net"
)

type ServerConfig struct {
	Host                  string
	Port                  int
	ConnectionTimeout     int
	ConnectionIdleTimeout int
	MaxConnections        int
}

var DefaultConfig = &ServerConfig{
	Host:                  "localhost",
	Port:                  6379,
	ConnectionTimeout:     10000,
	ConnectionIdleTimeout: 2000,
	MaxConnections:        2000,
}

type Server struct {
	listener net.Listener
	close    chan struct{}
	handlers map[string]func(ctx context.Context, args *resp.AnyResp) (interface{}, error)
	config   *ServerConfig
}

func getCommand(expression *resp.AnyResp) (string, error) {
	switch expression.I.(type) {
	case resp.SimpleString:
		return expression.I.(resp.SimpleString).S, nil
	case resp.BulkString:
		return string(expression.I.(resp.BulkString).S), nil
	case resp.RespArray:
		elem := expression.I.(resp.RespArray).A[0].(resp.RespMarshaler)
		return getCommand(&resp.AnyResp{I: elem})
	}
	return "", fmt.Errorf("invalid command type: %T", expression.I)
}

func (s *Server) parser(con net.Conn) {
	reader := bufio.NewReader(con)
	expression := resp.AnyResp{}
	err := expression.UnmarshalRESP(reader)
	if err != nil {
		panic(err)
	}
	command, err := getCommand(&expression)
	fmt.Printf("command: %q\n", command)
	if err != nil {
		resp.SimpleError{err.Error()}.MarshalRESP(con)
	}
	handler, ok := s.handlers[command]
	if !ok {
		resp.SimpleError{fmt.Sprintf("unknown command: %s", command)}.MarshalRESP(con)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res, err := handler(ctx, &expression)
	if err != nil {
		resp.SimpleError{err.Error()}.MarshalRESP(con)
	}
	fmt.Println("response: ", res)
	resp.AnyResp{res, false}.MarshalRESP(con)
	defer con.Close()
}

func (s *Server) RegisterHandler(command string, handler func(context.Context, *resp.AnyResp) (interface{}, error)) {
	s.handlers[command] = handler
}

func New(config *ServerConfig) (*Server, error) {
	if config == nil {
		config = DefaultConfig
	}
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", config.Host, config.Port))
	if err != nil {
		return nil, err
	}
	return &Server{
		listener: listener,
		close:    make(chan struct{}),
		handlers: make(map[string]func(ctx context.Context, args *resp.AnyResp) (interface{}, error)),
		config:   config,
	}, err
}

func (s Server) ListenAndServe() error {
	for {
		select {
		case <-s.close:
			return nil
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				return err
			}
			go s.parser(conn)
		}
	}

	panic("unreachable")
}

func (s *Server) Close() error {
	return s.listener.Close()
}
