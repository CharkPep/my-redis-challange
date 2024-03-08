package repl

import (
	"net"
	"testing"
)

func TestSmokePingMaster(t *testing.T) {
	client, server := net.Pipe()
	repl := &ReplicaOf{
		conn: client,
		host: "localhost",
		port: 6379,
	}

	go func() {
		err := repl.pingMaster()
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		client.Close()
	}()
	t.Log("pingMaster() test passed")
	buf := make([]byte, 16)
	n, err := server.Read(buf)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if string(buf[:n]) != "*1\r\n$4\r\nPING\r\n" {
		t.Errorf("expected PING, got %q", string(buf))
	}
	_, err = server.Write([]byte("+PONG\r\n"))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

}
