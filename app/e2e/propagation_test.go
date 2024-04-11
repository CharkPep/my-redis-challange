package e2e

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/handlers"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestReadIoTimeout(t *testing.T) {
	t.Skip()
	SetupMaster(t, MASTER_PORT)
	client, err := net.DialTimeout("tcp", ":6379", 5*time.Second)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	buff := make([]byte, 1024)
	var (
		n int
	)
	n, err = client.Read(buff)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	simpleErr := resp.SimpleError{}
	reader := bufio.NewReader(bytes.NewBuffer(buff[:n]))
	if _, err = simpleErr.UnmarshalRESP(reader); err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
}

func TestSingleReplicaPropagation(t *testing.T) {
	_, router := SetupMasterWithReplicationHandlers(t, MASTER_PORT)
	router.RegisterHandler("set", lib.ReplWrapper{Next: lib.HandleFunc(handlers.HandleSet)})
	_, replicaReader, _, err := ConnectReplica(fmt.Sprintf(":%d", MASTER_PORT))
	if err != nil {
		t.Fatalf("Failed to connect replica: %s", err)
	}

	t.Logf("Connected replica")
	client, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", MASTER_PORT), 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	buf := make([]byte, 1024)
	expect := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	client.Write([]byte(expect))
	n, err := client.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		t.Errorf("expected +OK, got %q", string(buf[:n]))
	}

	n, err = replicaReader.Read(buf)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if string(buf[:n]) != expect {
		t.Errorf("Propogation failed, expected %q, got %q", expect, string(buf[:n]))
	}
}

func TestMultiReplicaPropagation(t *testing.T) {
	N := 4
	_, router := SetupMasterWithReplicationHandlers(t, MASTER_PORT)
	router.RegisterHandler("set", lib.ReplWrapper{Next: lib.HandleFunc(handlers.HandleSet)})

	replicas := make([]struct {
		w io.Writer
		r *bufio.Reader
	}, 0, 4)
	for i := 0; i < N; i++ {
		replicaWriter, replicaReader, _, err := ConnectReplica(fmt.Sprintf(":%d", MASTER_PORT))
		if err != nil {
			t.Fatalf("Failed to connect replica: %s", err)
		}
		replicas = append(replicas, struct {
			w io.Writer
			r *bufio.Reader
		}{w: replicaWriter, r: replicaReader})
		t.Logf("Connected replica %d", i)
	}

	commands := []string{
		"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1\r\n1\r\n",
		"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$1\r\n2\r\n",
		"*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$1\r\n3\r\n",
	}
	done := make(chan struct{}, len(commands))
	for _, c := range commands {
		c := c
		go func(c string) {
			client, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", MASTER_PORT), 5*time.Second)
			r := bufio.NewReader(client)
			if err != nil {
				t.Errorf("unexpected error: %s", err)
			}

			_, err = client.Write([]byte(c))
			if err != nil {
				t.Errorf("unexpected error: %s", err)
			}

			res := resp.SimpleString{}
			if _, err := res.UnmarshalRESP(r); err != nil {
				t.Error(err)
			}

			if res.S != "OK" {
				t.Errorf("exptected OK, got %s", res.S)
			}

			done <- struct{}{}
		}(c)
	}

	for i := 0; i < len(commands); i++ {
		<-done
	}

	for _, repl := range replicas {
		buf := make([]byte, 1024)
		n, err := repl.r.Read(buf)
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		for _, c := range commands {
			if !strings.Contains(string(buf[:n]), c) {
				t.Errorf("Expected %q to contain %q", buf[:n], c)
			}
		}
	}
}

func TestPropagation(t *testing.T) {
	const REPLICA_PORT = 6800
	_, routerMaster := SetupMasterWithReplicationHandlers(t, MASTER_PORT)
	routerMaster.RegisterHandler("set", lib.ReplWrapper{Next: lib.HandleFunc(handlers.HandleSet)})
	routerMaster.RegisterHandlerFunc("get", handlers.HandleGet)
	_, routerReplica := SetupReplicaOf(t, REPLICA_PORT, fmt.Sprintf(":%d", MASTER_PORT))
	routerReplica.RegisterHandler("set", lib.ReplWrapper{Next: lib.HandleFunc(handlers.HandleSet)})
	routerReplica.RegisterHandlerFunc("get", handlers.HandleGet)
	master, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", MASTER_PORT), time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	type kv struct {
		key string
		val string
	}
	commands := []kv{
		{"foo", "123"},
		{"bar", "456"},
		{"baz", "789"},
	}
	for _, c := range commands {
		master.Write([]byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$3\r\n%s\r\n$3\r\n%s\r\n", c.key, c.val)))
	}

	r := bufio.NewReader(master)
	res := resp.SimpleString{}
	if _, err := res.UnmarshalRESP(r); err != nil {
		t.Error(err)
	}

	if res.S != "OK" {
		t.Errorf("exptected OK, got %s", res.S)
	}

	replica, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", REPLICA_PORT), time.Second*5)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	rReplica := bufio.NewReader(replica)
	time.Sleep(1000 * time.Millisecond)
	for _, c := range commands {
		_, err = replica.Write([]byte(fmt.Sprintf("*2\r\n$3\r\nget\r\n$3\r\n%s\r\n", c.key)))
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		res := resp.BulkString{}
		if _, err := res.UnmarshalRESP(rReplica); err != nil {
			t.Error(err)
		}

		if !bytes.Equal(res.S, []byte(c.val)) {
			t.Errorf("expected %s, got %s", c.val, res.S)
		}
	}
}
