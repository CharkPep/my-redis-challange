package lib

import (
	"bytes"
	"context"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib/repl"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"
)

var (
	READ_TIMEOUT = 10 * time.Second

	PROPAGATION_CONSUMERS = 1
)

type ServerConfig struct {
	Host                   string
	Port                   int
	ConnectionReadTimeout  time.Duration
	ConnectionWriteTimeout time.Duration
	ReplicaOf              string
	ReplicationConfig      *repl.ReplicationConfig
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
		MasterReplOffset:   atomic.Uint64{},
		SecondReplOffset:   atomic.Uint64{},
		ConnectedSlaves:    atomic.Uint64{},
		ReplBacklogActive:  0,
		ReplBacklogSize:    1048576,
		ReplBacklogFirst:   0,
		ReplBacklogHistlen: 0,
	},
}

type HandleRESP interface {
	HandleResp(ctx context.Context, req *RESPRequest) (interface{}, error)
}

type Server struct {
	logger      *log.Logger
	listener    net.Listener
	close       chan struct{}
	config      *ServerConfig
	router      *Router
	propagation chan *repl.REPLRequest
	replicaOf   *repl.ReplicaOf
	slaves      []*repl.Slave
}

func (s *Server) PropagateToAll(buff []byte) {
	s.logger.Printf("Propagating to all slaves, %d", len(s.slaves))
	for _, r := range s.slaves {
		if _, err := r.Propagate(buff); err != nil {
			s.logger.Printf("Error writing to replica: %s", err)
		}
	}
}

func New(config *ServerConfig, router *Router) (*Server, error) {
	if config == nil {
		config = DefaultConfig
	}
	logger := log.New(os.Stdout, fmt.Sprintf("master %d: ", config.Port), log.Lmicroseconds|log.Lshortfile)
	var propagation chan *repl.REPLRequest = nil
	if config.ReplicaOf != "" {
		logger.SetPrefix("replica")
		propagation = make(chan *repl.REPLRequest, 100)
	}

	replID := bytes.NewBuffer(make([]byte, 0, 40))
	utils.RandomAlphanumericString(replID, 40)
	config.ReplicationConfig.MasterReplid = string(replID.Bytes())

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", config.Host, config.Port))
	if err != nil {
		return nil, err
	}
	return &Server{
		logger:      logger,
		listener:    listener,
		router:      router,
		close:       make(chan struct{}),
		slaves:      make([]*repl.Slave, 0, 4),
		config:      config,
		propagation: propagation,
	}, err
}

func (s *Server) ConnectMaster() error {
	if s.config.ReplicaOf != "" {
		s.config.ReplicationConfig.Role = "slave"
		master, err := repl.NewReplicaOf(s.config.ReplicaOf, fmt.Sprint(s.config.Port), s.propagation)
		if err != nil {
			s.logger.Printf("Failed to connect to master %v: %s", s.config.ReplicaOf, err)
			return err
		}
		s.replicaOf = master
		go s.initPropagationConsumptionFromMaster()
	}
	return nil
}

func (s *Server) ListenAndServe() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		go func(conn net.Conn) {
			if err = conn.SetDeadline(time.Now().Add(time.Second * 10)); err != nil {
				s.logger.Printf("Error setting read deadline: %s", err)
				return
			}
			defer conn.Close()
			s.logger.Printf("Accepted connection from %s", conn.RemoteAddr())
			NewRequest(conn, s).Handle(s.router)
		}(conn)
	}
}

func (s *Server) initPropagationConsumptionFromMaster() {
	for i := 0; i < PROPAGATION_CONSUMERS; i++ {
		go func(i int) {
			for {
				select {
				case _, ok := <-s.close:
					if !ok {
						s.logger.Printf("Closing consumer %d", i)
						return
					}
				case req := <-s.propagation:
					handler, err := s.router.ResolveRequest(req.Args)
					if err != nil {
						s.logger.Printf("Error resolving request: %s", err)
						continue
					}

					req.Args.A = req.Args.A[1:]
					_, err = handler.HandleResp(context.Background(), &RESPRequest{
						Args:          req.Args,
						Logger:        s.logger,
						s:             s,
						W:             req.Writer,
						IsPropagation: true,
					})
					log.Printf("Propagated %v in replica %d", req, s.config.Port)
					if err != nil {
						s.logger.Printf("ERROR: propagating to replica: %s", err)
					}
					s.config.ReplicationConfig.MasterReplOffset.Add(uint64(req.N))
					log.Printf("Offset: %d", s.config.ReplicationConfig.MasterReplOffset.Load())
				}
			}
		}(i)
	}
}

func (s *Server) Close() error {
	s.logger.Println("Closing server")
	close(s.close)
	return s.listener.Close()
}
