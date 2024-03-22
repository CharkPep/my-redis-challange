package repl

import (
	"bufio"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type Slave struct {
	conn   net.Conn
	logger *log.Logger
	mu     sync.Mutex
	offset uint64
	r      *bufio.Reader
}

func (r *Slave) GetAddr() net.Addr {
	return r.conn.RemoteAddr()
}

func (r *Slave) GetOffset() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.offset
}

func NewReplica(conn net.Conn, i string) *Slave {
	return &Slave{
		conn:   conn,
		logger: log.New(os.Stdout, fmt.Sprintf("slave %s: ", i), log.Lmicroseconds|log.Lshortfile),
		mu:     sync.Mutex{},
		r:      bufio.NewReader(conn),
	}
}

func (r *Slave) Propagate(b []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.conn.Write(b)
}

func (r *Slave) GetAck(timeout time.Duration) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.conn.SetDeadline(time.Now().Add(timeout))
	defer func() {
		r.conn.SetDeadline(time.Time{})
		r.logger.Printf("Reset read deadline")
	}()
	if _, err := (&resp.Array{
		A: []resp.Marshaller{
			resp.BulkString{S: []byte("REPLCONF")},
			resp.BulkString{S: []byte("GETACK")},
			resp.BulkString{S: []byte("*")},
		},
	}).MarshalRESP(r.conn); err != nil {
		return r.offset, fmt.Errorf("failed to send getack")
	}

	offsetRes := resp.Array{}
	if _, err := offsetRes.UnmarshalRESP(r.r); err != nil {
		return r.offset, err
	}

	if len(offsetRes.A) < 3 {
		return r.offset, fmt.Errorf("invalid offset response")
	}

	if v, ok := offsetRes.A[2].(resp.BulkString); ok {
		offset, err := strconv.ParseInt(string(v.S), 10, 64)
		if err != nil {
			return r.offset, err
		}

		r.offset = uint64(offset)
		r.logger.Printf("Got ack %d", r.offset)
		return uint64(offset), nil
	}

	return r.offset, fmt.Errorf("invalid offset response")
}
