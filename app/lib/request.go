package lib

import (
	"bufio"
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/storage"
	"io"
	"log"
	"net"
	"os"
	"reflect"
	"sync"
	"time"
)

var loggerPool = sync.Pool{
	New: func() any {
		return log.New(os.Stdout, "", log.Lmicroseconds|log.Lshortfile)
	},
}

type Handler interface {
	HandleResp(ctx context.Context, req *RESPRequest) (interface{}, error)
}

type HandleFunc func(ctx context.Context, req *RESPRequest) (interface{}, error)

func (f HandleFunc) HandleResp(ctx context.Context, req *RESPRequest) (interface{}, error) {
	return f(ctx, req)
}

type RESPRequest struct {
	Logger      *log.Logger
	closed      bool
	conn        net.Conn
	W           io.Writer
	s           *RedisServer
	r           *bufio.Reader
	Db          *storage.RedisDataTypes
	Config      *ServerConfig
	Args        *resp.Array
	RemoteAddr  net.Addr
	Propagation bool
}

func NewRequest(rwc net.Conn, s *RedisServer) *RESPRequest {
	req := &RESPRequest{
		conn:       rwc,
		RemoteAddr: rwc.RemoteAddr(),
		W:          rwc,
		r:          bufio.NewReader(rwc),
		s:          s,
		Config:     s.config,
		Args:       &resp.Array{},
	}

	logger, ok := loggerPool.Get().(*log.Logger)
	if !ok {
		panic(fmt.Sprintf("expected to get logger from pool, but got %T", logger))
	}

	logger.SetPrefix(fmt.Sprintf("%srequest-%s", req.s.logger.Prefix(), req.RemoteAddr))
	logger.SetFlags(req.s.logger.Flags())
	req.Logger = logger
	if err := req.SetDb(0); err != nil {
		req.Logger.Printf("unexpected error: %s", err)
	}

	return req
}

func (req *RESPRequest) SetDb(idx int) error {
	dbAny, _ := req.s.db.LoadOrStore(idx, storage.NewDb(idx))
	db, ok := dbAny.(*storage.RedisDataTypes)
	if !ok {
		return fmt.Errorf("unexpected error asserting new db type")
	}

	req.Db = db
	return nil
}

func (req *RESPRequest) Handle(router *Router) {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		start       time.Time
		n           int
		err         error
	)

	defer cancel()
	defer loggerPool.Put(req.Logger)
	for {
		start = time.Now()
		req.Logger.Printf("Reading request from %s", req.RemoteAddr)
		n, err = req.read(req.r)
		if err == io.EOF {
			req.Logger.Printf("Connection closed by %s", req.RemoteAddr)
			break
		}

		req.Logger.Printf("read %d bytes from %s", n, req.RemoteAddr)
		if err != nil {
			resp.SimpleError{E: err.Error()}.MarshalRESP(req.W)
			continue
		}
		req.Logger.Printf("Request: %s from %s", req.Args, req.RemoteAddr)
		handler, err := router.ResolveRequest(req.Args)
		if err != nil {
			resp.SimpleError{fmt.Sprintf("%s", err)}.MarshalRESP(req.W)
			continue
		}

		req.Args.A = req.Args.A[1:]
		res, err := handler.HandleResp(ctx, req)
		if err != nil {
			resp.SimpleError{E: err.Error()}.MarshalRESP(req.W)
			continue
		}

		if res != nil {
			req.Logger.Printf("Response: %s, type %s to %s in %s", res, reflect.TypeOf(res), req.RemoteAddr, time.Now().Sub(start))
			if _, err = (resp.Any{I: res}.MarshalRESP(req.W)); err != nil {
				return
			}
		}
	}
}

func (req *RESPRequest) read(r *bufio.Reader) (n int, err error) {
	if n, err = req.Args.UnmarshalRESP(r); err != nil {
		return n, err
	}
	return n, err
}
