package lib

import (
	"bufio"
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"io"
	"log"
	"net"
	"os"
)

type RESPRequest struct {
	Logger *log.Logger
	W      io.Writer
	// for internal use
	r             *bufio.Reader
	s             *Server
	Args          *resp.Array
	RAddr         net.Addr
	conn          net.Conn
	IsPropagation bool
}

func NewRequest(rwc net.Conn, s *Server) *RESPRequest {
	return &RESPRequest{
		Logger: log.New(os.Stdout, fmt.Sprintf("%srequest-%s", s.logger.Prefix(), rwc.RemoteAddr()), s.logger.Flags()),
		conn:   rwc,
		RAddr:  rwc.RemoteAddr(),
		W:      rwc,
		s:      s,
		r:      bufio.NewReader(rwc),
		Args:   &resp.Array{},
	}
}

func (req *RESPRequest) Handle(router *Router) {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		n           int
		err         error
	)
	defer cancel()
	for {
		req.Logger.Printf("Reading request from %s", req.RAddr)
		n, err = req.Read(req.r)
		if err == io.EOF {
			req.Logger.Printf("Connection closed by %s", req.RAddr)
			break
		}
		req.Logger.Printf("Read %d bytes from %s", n, req.RAddr)
		if err != nil {
			resp.SimpleError{E: err.Error()}.MarshalRESP(req.W)
			return
		}
		req.Logger.Printf("Request: %s from %s", req.Args, req.RAddr)
		handler, err := router.ResolveRequest(req.Args)
		if err != nil {
			resp.SimpleError{fmt.Sprintf("%s", err)}.MarshalRESP(req.W)
			return
		}

		req.Args.A = req.Args.A[1:]
		res, err := handler.HandleResp(ctx, req)
		if err != nil {
			resp.SimpleError{E: err.Error()}.MarshalRESP(req.W)
			return
		}

		if res != nil {
			req.Logger.Printf("Response: %s to %s", res, req.RAddr)
			if _, err = (resp.AnyResp{I: res}.MarshalRESP(req.W)); err != nil {
				return
			}
		}
	}
}

func (req *RESPRequest) Read(r *bufio.Reader) (n int, err error) {
	req.Logger.Printf("Reading from %s", req.RAddr)
	if n, err = req.Args.UnmarshalRESP(r); err != nil {
		return n, err
	}
	return n, err
}
