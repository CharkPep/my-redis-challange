package repl

import (
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"net"
	"time"
)

type ReplicaOf struct {
	conn net.Conn
	host string
	port int
}

func NewReplicaOf(host string, remotePort, listeningPort int) (*ReplicaOf, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, remotePort))
	conn.SetReadDeadline(time.Now().Add(time.Second * 10))
	conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
	if err != nil {
		return nil, err
	}

	defer conn.Close()
	repl := &ReplicaOf{
		conn: conn,
		host: host,
		port: remotePort,
	}
	if err = repl.pingMaster(); err != nil {
		return nil, err
	}
	if err = repl.replConfPort(listeningPort); err != nil {
		return nil, err
	}
	if err = repl.replConfCapa(); err != nil {
		return nil, err
	}

	if err = repl.pSync(); err != nil {
		return nil, err
	}

	return repl, nil
}

func (r *ReplicaOf) pingMaster() error {
	var err error
	if err = (resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("PING")}}}.MarshalRESP(r.conn)); err != nil {
		return err
	}
	buf := make([]byte, 16)
	var n int
	if n, err = r.conn.Read(buf); err != nil {
		return err
	}

	if string(buf[:n]) != "+PONG\r\n" {
		return fmt.Errorf("expected PONG, got %q", string(buf))
	}
	return nil
}

func (r *ReplicaOf) replConfPort(selfPort int) error {
	var err error
	if err = (resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("REPLCONF")}, resp.BulkString{S: []byte("listening-port")}, resp.BulkString{S: []byte(fmt.Sprint(selfPort))}}}.MarshalRESP(r.conn)); err != nil {
		return err
	}
	buf := make([]byte, 16)
	var n int
	if n, err = r.conn.Read(buf); err != nil {
		return err
	}

	if string(buf[:n]) != "+OK\r\n" {
		return fmt.Errorf("expected OK, got %q", string(buf))
	}
	return nil
}

func (r *ReplicaOf) replConfCapa() error {
	var err error
	if err = (resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("REPLCONF")}, resp.BulkString{S: []byte("capa")}, resp.BulkString{S: []byte("psync2")}}}.MarshalRESP(r.conn)); err != nil {
		return err
	}
	buf := make([]byte, 16)
	var n int
	if n, err = r.conn.Read(buf); err != nil {
		return err
	}

	if string(buf[:n]) != "+OK\r\n" {
		return fmt.Errorf("expected OK, got %q", string(buf))
	}
	return nil
}

func (r *ReplicaOf) pSync() error {
	var err error
	if err = (resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("PSYNC")}, resp.BulkString{S: []byte("?")}, resp.BulkString{S: []byte("-1")}}}.MarshalRESP(r.conn)); err != nil {
		return err
	}
	buf := make([]byte, 16)
	if _, err = r.conn.Read(buf); err != nil {
		return err
	}

	return nil
}
