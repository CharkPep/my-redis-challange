package repl

import (
	"net"
)

type Replica struct {
	conn net.Conn
	//TODO
	//buffered []resp.Array
}

func (r Replica) GetAddr() net.Addr {
	return r.conn.RemoteAddr()
}

func NewReplica(conn net.Conn) *Replica {
	return &Replica{
		conn: conn,
	}
}

func (r Replica) Propagate(b []byte) (int, error) {
	return r.conn.Write(b)
}
