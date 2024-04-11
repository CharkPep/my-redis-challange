package lib

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/replication"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

var (
	READ_TIMEOUT = 10 * time.Second

	// TODO: add mutex to propagation
	PROPAGATION_CONSUMERS = 1
)

type RedisServer struct {
	mu       *sync.RWMutex
	logger   *log.Logger
	listener net.Listener
	close    chan struct{}
	config   *ServerConfig
	router   *Router
	rdb      *encoding.Rdb
	// map[int]*storage.RedisDataTypes
	db          *sync.Map
	propagation chan *replication.REPLRequest
	replicaOf   *replication.ReplicaOf
	slaves      []*replication.Slave
}

func New(config *ServerConfig, router *Router) (*RedisServer, error) {
	if config == nil {
		config = defaultConfig
	}

	logger := log.New(os.Stdout, fmt.Sprintf("master %d: ", config.Port), log.Lmicroseconds|log.Lshortfile)
	var propagation chan *replication.REPLRequest = nil
	if config.ReplicaOf != "" {
		logger.SetPrefix("slave")
		propagation = make(chan *replication.REPLRequest, 100)
	}

	replID := bytes.NewBuffer(make([]byte, 0, 40))
	utils.RandomAlphanumericString(replID, 40)
	config.ReplicationConfig.MasterReplid = string(replID.Bytes())
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", config.Host, config.Port))
	if err != nil {
		return nil, err
	}

	db := sync.Map{}
	s := RedisServer{
		mu:          &sync.RWMutex{},
		logger:      logger,
		listener:    listener,
		router:      router,
		db:          &db,
		close:       make(chan struct{}),
		slaves:      make([]*replication.Slave, 0, 4),
		config:      config,
		propagation: propagation,
	}
	s.loadDb()
	return &s, nil
}

func (s *RedisServer) loadDb() {
	if s.config.PersistenceConfig == nil || (s.config.PersistenceConfig.Dir == "" && s.config.PersistenceConfig.File == "") {
		s.logger.Printf("rdb file is not configured")
		return
	}

	s.rdb = encoding.NewRdb(s.db)
	absp, err := filepath.Abs(path.Join(s.config.PersistenceConfig.Dir, s.config.PersistenceConfig.File))
	if err != nil {
		os.Exit(1)
	}

	rdbf, err := os.Open(absp)
	defer rdbf.Close()
	if err != nil {
		s.logger.Printf("Failed to open rdb file: %s, creating new one\n", err)
		f, err := os.Create(absp)
		if err != nil {
			s.logger.Panicf("Failed to create rdb file: %s", err)
		}

		if _, err = s.rdb.MarshalRESP(f); err != nil {
			s.logger.Fatal(err)
		}

		s.logger.Printf("Created new rdb file: %s", absp)
	} else {
		s.logger.Printf("reding rdb %s", absp)
		r := bufio.NewReader(rdbf)
		if err := s.rdb.Load(r); err != nil {
			fmt.Printf("Failed to unmarshal rdb: %s", err)
			os.Exit(1)
		}
	}

}

func (s *RedisServer) ListenAndServe() error {
	s.logger.Printf("Listeing on port %d", s.config.Port)
	for {
		conn, err := s.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}

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

func (s *RedisServer) Close() error {
	s.logger.Println("Closing server")
	close(s.close)
	return s.listener.Close()
}
