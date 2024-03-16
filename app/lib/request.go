package lib

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"io"
	"log"
	"net"
)

type RESPRequest struct {
	Logger *log.Logger
	Conn   net.Conn
	S      *Server
	Args   *resp.Array
}

func NewRequest(rwc net.Conn, s *Server) *RESPRequest {
	return &RESPRequest{
		Logger: s.logger,
		Conn:   rwc,
		S:      s,
		Args:   &resp.Array{},
	}
}

func (req *RESPRequest) Handle() {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		n           int
		r           *bufio.Reader
		buff        = make([]byte, 1024)
		err         error
	)
	defer cancel()
	for {
		req.Logger.Printf("Waiting for data from %s", req.Conn.RemoteAddr())
		if n, err = req.Conn.Read(buff); err != nil && err != io.EOF {
			req.Logger.Printf("Error reading from %s: %s", req.Conn.RemoteAddr(), err)
			return
		}

		if err == io.EOF {
			req.Logger.Printf("Connection closed by %s", req.Conn.RemoteAddr())
			return
		}

		req.Logger.Printf("Received %q from %s", buff[:n], req.Conn.RemoteAddr())
		r = bufio.NewReader(bytes.NewBuffer(buff[:n]))
		for peeked, err := r.Peek(1); err == nil && len(peeked) > 0; peeked, err = r.Peek(1) {
			req.Logger.Printf("Reading request from %s", req.Conn.RemoteAddr())
			err = req.Read(r)
			if err == io.EOF {
				req.Logger.Printf("Connection closed by %s", req.Conn.RemoteAddr())
				break
			}
			if err != nil {
				resp.SimpleError{E: err.Error()}.MarshalRESP(req.Conn)
				return
			}
			req.Logger.Printf("Request: %s from %s", req.Args, req.Conn.RemoteAddr())
			handler, err := req.S.router.ResolveRequest(req.Args)
			if err != nil {
				resp.SimpleError{fmt.Sprintf("%s", err)}.MarshalRESP(req.Conn)
				return
			}

			req.Args.A = req.Args.A[1:]
			res, err := handler.HandleResp(ctx, req)
			if err != nil {
				resp.SimpleError{E: err.Error()}.MarshalRESP(req.Conn)
				return
			}

			if res != nil {
				req.Logger.Printf("Response: %v to %s", res, req.Conn.RemoteAddr())
				if err = (resp.AnyResp{I: res}.MarshalRESP(req.Conn)); err != nil {
					return
				}
			}
		}
	}
}

func (req *RESPRequest) Read(r *bufio.Reader) (err error) {
	req.Logger.Printf("Reading from %s", req.Conn.RemoteAddr())
	if err = req.Args.UnmarshalRESP(r); err != nil {
		return err
	}
	return nil
}
