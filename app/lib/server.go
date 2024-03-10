package lib

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/repl"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
	"log"
	"net"
	"time"
)

type ServerConfig struct {
	Host                   string
	Port                   int
	ConnectionReadTimeout  time.Duration
	ConnectionWriteTimeout time.Duration
	ReplicationConfig      *repl.ReplicationConfig
	ReplicaOf              *repl.ReplicaOf
}

func GetDefaultConfig() *ServerConfig {
	return DefaultConfig
}

var DefaultConfig = &ServerConfig{
	Host:                   "localhost",
	Port:                   6379,
	ConnectionReadTimeout:  time.Second * 2,
	ConnectionWriteTimeout: time.Second * 2,
	ReplicationConfig: &repl.ReplicationConfig{
		Role:               "master",
		MasterReplOffset:   0,
		SecondReplOffset:   -1,
		ConnectedSlaves:    0,
		ReplBacklogActive:  0,
		ReplBacklogSize:    1048576,
		ReplBacklogFirst:   0,
		ReplBacklogHistlen: 0,
	},
}

type HandleRESP interface {
	HandleResp(ctx context.Context, args *resp.Array) (interface{}, error)
}

type Server struct {
	logger   *log.Logger
	listener net.Listener
	close    chan struct{}
	handlers map[string]*HandleRESP
	config   *ServerConfig
	replOf   *repl.ReplicaOf
	replicas *repl.ReplicaManager
}

func getCommand(args *[]resp.Marshaller) (string, error) {
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

func (s *Server) parse(con net.Conn) {
	defer con.Close()
	err := con.SetReadDeadline(time.Now().Add(s.config.ConnectionReadTimeout))
	if err != nil {
		resp.SimpleError{E: err.Error()}.MarshalRESP(con)
		log.Printf("error: %s", err.Error())
		return
	}
	err = con.SetWriteDeadline(time.Now().Add(s.config.ConnectionWriteTimeout))
	if err != nil {
		resp.SimpleError{E: err.Error()}.MarshalRESP(con)
		log.Printf("error: %s", err.Error())
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		select {
		case _, ok := <-s.close:
			if !ok {
				return
			}
		default:
			buff := make([]byte, 1024)
			_, err := con.Read(buff)
			if err != nil {
				resp.SimpleError{E: err.Error()}.MarshalRESP(con)
				return
			}
			reader := bufio.NewReader(bytes.NewReader(buff))
			var args resp.Array
			err = args.UnmarshalRESP(reader)
			if err != nil {
				resp.SimpleError{E: err.Error()}.MarshalRESP(con)
				return
			}
			command, err := getCommand(&args.A)
			if err != nil {
				resp.SimpleError{err.Error()}.MarshalRESP(con)
			}
			handler, ok := s.handlers[command]
			if !ok {
				resp.SimpleError{fmt.Sprintf("unknown command: %s", command)}.MarshalRESP(con)
				return
			}
			log.Printf("Handling command: %s, from %s ", command, con.RemoteAddr().String())
			ctx = context.WithValue(ctx, "ctx", map[string]interface{}{"conn": con})
			args.A = args.A[1:]
			res, err := (*handler).HandleResp(ctx, &args)
			if err != nil {
				resp.SimpleError{err.Error()}.MarshalRESP(con)
			}

			req, ok := ctx.Value("ctx").(map[string]interface{})
			if !ok {
				resp.SimpleError{E: fmt.Sprintf("invalid context, expected map[string]interface{}, got %T", req)}.MarshalRESP(con)
			}
			// Escape hatch from returning a bulk nil or nil array
			if req["encode"] != nil {
				if encodeBulkStringNil, ok := req["encodeBulkStringNil"]; ok && encodeBulkStringNil.(bool) {
					resp.AnyResp{res, true}.MarshalRESP(con)
					return
				}

				return
			}

			resp.AnyResp{res, false}.MarshalRESP(con)
		}
	}
}

func (s *Server) RegisterHandler(command string, handler HandleRESP) {
	s.handlers[command] = &handler
}

func New(config *ServerConfig, replicaManger *repl.ReplicaManager) (*Server, error) {
	if config == nil {
		config = DefaultConfig
	}

	if config.ReplicationConfig != nil {
		replID := bytes.NewBuffer(make([]byte, 0, 40))
		utils.RandomAlphanumericString(replID, 40)
		config.ReplicationConfig.MasterReplid = string(replID.Bytes())
	}
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", config.Host, config.Port))
	if err != nil {
		return nil, err
	}
	return &Server{
		listener: listener,
		close:    make(chan struct{}),
		replicas: replicaManger,
		handlers: make(map[string]*HandleRESP),
		config:   config,
	}, err
}

func (s *Server) ListenAndServe() error {
	for {
		select {
		case _, ok := <-s.close:
			if !ok {
				return nil
			}
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				return err
			}
			go s.parse(conn)
		}
	}
	panic("unreachable")
}

func (s *Server) Close() error {
	log.Println("Closing server")
	close(s.close)
	return s.listener.Close()
}
