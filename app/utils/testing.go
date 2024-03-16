package utils

import (
	"bufio"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"net"
	"time"
)

func EstablishReplicaMaster(replica net.Conn) error {
	replica.Write([]byte("*1\r\n$4\r\nping\r\n"))
	buf := make([]byte, 1024)
	n, _ := replica.Read(buf)
	if string(buf[:n]) != "+PONG\r\n" {
		return fmt.Errorf("expected +PONG, got %q", string(buf[:n]))
	}

	replica.Write([]byte("*3\r\n$8\r\nreplconf\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))
	n, _ = replica.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		return fmt.Errorf("expected +OK on replconf port, got %q", string(buf[:n]))
	}

	replica.Write([]byte("*3\r\n$8\r\nreplconf\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	n, _ = replica.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		return fmt.Errorf("expected +OK on replconf capa, got %q", string(buf[:n]))
	}

	replica.Write([]byte("*3\r\n$5\r\npsyncr\n$1\r\n?\r\n$2\r\n-1\r\n"))
	r := bufio.NewReader(replica)
	if err := (&resp.SimpleString{}).UnmarshalRESP(r); err != nil {
		return err
	}
	if err := (&resp.Rdb{}).UnmarshalRESP(r); err != nil {

	}
	return nil
}

func ConnectReplica(host string) (net.Conn, error) {
	replica, err := net.DialTimeout("tcp", host, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("unexpected error: %s", err)
	}
	if err := EstablishReplicaMaster(replica); err != nil {
		return nil, err
	}
	return replica, err
}
