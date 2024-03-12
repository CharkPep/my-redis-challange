package lib

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/repl"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
	"io"
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
	var (
		args        resp.Array
		n           int
		err         error
		ctx, cancel = context.WithCancel(context.Background())
		buff        = make([]byte, 1024)
	)
	defer cancel()
	defer con.Close()
	if err = con.SetReadDeadline(time.Now().Add(time.Second * 10)); err != nil {
		log.Printf("Error setting read deadline: %s", err)
		return
	}
	for {
		select {
		case _, ok := <-s.close:
			if !ok {
				return
			}
		default:
			log.Printf("Reading from connection: %s", con.RemoteAddr())
			n, err = con.Read(buff)
			if err != nil {
				log.Printf("Unexpected error while reading from %s, %s", con.RemoteAddr(), err)
				resp.SimpleError{E: err.Error()}.MarshalRESP(con)
				return
			}
			log.Printf("Got %q, from %s", buff[:n], con.RemoteAddr())
			reader := bufio.NewReader(bytes.NewReader(buff[:n]))
			for {
				err = args.UnmarshalRESP(reader)
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Printf("Unexpected error while unmarshaling resp from %s: %s", con.RemoteAddr(), err)
					resp.SimpleError{E: err.Error()}.MarshalRESP(con)
					return
				}
				command, err := getCommand(&args.A)
				if err != nil {
					log.Printf("Unexpected error getting resp commnand from %s: %s", con.RemoteAddr(), err)
					resp.SimpleError{err.Error()}.MarshalRESP(con)
					return
				}
				handler, ok := s.handlers[command]
				if !ok {
					log.Printf("Unexpected error getting handler from %s", con.RemoteAddr())
					resp.SimpleError{fmt.Sprintf("unknown command: %s", command)}.MarshalRESP(con)
					return
				}
				log.Printf("Handling command: %s, from %s ", command, con.RemoteAddr().String())
				log.Printf("Args: %s", args)
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
						log.Printf("Reponse with encode nil")
						resp.AnyResp{res, true}.MarshalRESP(con)
						return
					}

					log.Printf("Disconnecting without sending reponse")
					return
				}

				log.Printf("Reponse to %s: %q", con.RemoteAddr(), res)
				resp.AnyResp{res, false}.MarshalRESP(con)
			}
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
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		go s.parse(conn)
	}
}

func (s *Server) Close() error {
	log.Println("Closing server")
	close(s.close)
	return s.listener.Close()
}
