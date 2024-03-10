package repl

import (
	"net"
	"strings"
)

type Replica struct {
	dial net.Conn
}

func (r Replica) GetPort() string {
	return strings.Split(r.dial.RemoteAddr().String(), ":")[1]
}

func (r Replica) GetAddress() string {
	return strings.Split(r.dial.RemoteAddr().String(), ":")[0]
}

func NewReplica(conn net.Conn) *Replica {
	return &Replica{
		dial: conn,
	}
}

func (repl *Replica) Send(b []byte) (int, error) {
	n, err := repl.dial.Write(b)
	if err != nil {
		return n, err
	}

	return n, nil
}
