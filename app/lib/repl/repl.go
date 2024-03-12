package repl

import (
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"net"
)

type Replica struct {
	conn net.Conn

	// Buffered propagation to replica to be send after replica read persistence file
	buffered []resp.Array
}

func (r Replica) GetAddr() net.Addr {
	return r.conn.LocalAddr()
}

func NewReplica(conn net.Conn) *Replica {
	return &Replica{
		conn: conn,
	}
}
