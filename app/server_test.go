package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
	"log"
	"net"
	"strings"
	"testing"
	"time"
)

var (
	server *lib.Server
	err    error
	PORT   = 6379
)

func SetupMaster(tb testing.TB) func(tb testing.TB) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	config := lib.DefaultConfig
	config.Port = PORT
	router := lib.NewRouter()
	RegisterHandlers(router)
	server, err = lib.New(config, router)
	if err != nil {
		panic(err)
	}
	go func() {
		server.ListenAndServe()
	}()
	tb.Logf("Starting master on port %d", PORT)
	return func(tb testing.TB) {
		server.Close()
	}
}

func SetupReplicaOf(tb testing.TB, replOf string, port int) (func(tb testing.TB), *lib.Server) {
	conf := lib.DefaultConfig
	conf.ReplicaOf = replOf
	conf.Port = port
	router := lib.NewRouter()
	RegisterHandlers(router)
	replServer, err := lib.New(conf, router)
	if err != nil {
		panic(err)
	}
	errChan := make(chan error, 1)
	go func() {
		if err := replServer.ConnectMaster(); err != nil {
			errChan <- err
			return
		}
		errChan <- nil
		replServer.ListenAndServe()
	}()
	if err := <-errChan; err != nil {
		tb.Fatalf("Failed to connect to master: %s", err)
	}
	tb.Logf("Starting replica on port %d", port)
	return func(tb testing.TB) {
		replServer.Close()
	}, replServer
}

func TestServerShouldAcceptConnection(t *testing.T) {
	teardown := SetupMaster(t)
	defer teardown(t)
	_, err := net.Dial("tcp", fmt.Sprintf(":%d", PORT))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestServerShouldReturnPong(t *testing.T) {
	teardown := SetupMaster(t)
	defer teardown(t)
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", PORT))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	conn.Write([]byte("*1\r\n$4\r\nping\r\n"))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if string(buf[:n]) != "+PONG\r\n" {
		t.Errorf("expected +PONG\n, got %s", string(buf[:n]))
	}
}

func TestServerShouldReturnEcho(t *testing.T) {
	type testCase struct {
		args   resp.Marshaller
		output string
	}
	tests := []testCase{
		{
			args:   resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("echo")}, resp.BulkString{S: []byte("foo")}}},
			output: "$3\r\nfoo\r\n",
		},
		{
			args: resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("echo")},
				resp.BulkString{S: []byte("foo")}, resp.BulkString{S: []byte("bar")}}},
			output: "$3\r\nfoo\r\n",
		},
		{
			args:   resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("echo")}, resp.BulkString{S: []byte("apples")}}},
			output: "$6\r\napples\r\n",
		},
		{
			args:   resp.Array{A: []resp.Marshaller{resp.BulkString{S: []byte("echo")}}},
			output: "-ERR wrong number of arguments for command\r\n",
		},
	}
	teardown := SetupMaster(t)
	defer teardown(t)
	t.Run("echo", func(t *testing.T) {
		for i, test := range tests {
			t.Run(fmt.Sprintf("echo:%d", i), func(ts *testing.T) {
				test := test
				ts.Parallel()
				conn, err := net.Dial("tcp", ":6379")
				defer conn.Close()
				if err != nil {
					ts.Errorf("unexpected error: %s", err)
				}
				buff := bytes.NewBuffer([]byte{})
				_, err = test.args.MarshalRESP(buff)
				if err != nil {
					ts.Errorf("unexpected error: %s", err)
				}
				if _, err = conn.Write(buff.Bytes()); err != nil {
					ts.Errorf("unexpected error: %s", err)
				}
				buf := make([]byte, 1024)
				n, err := conn.Read(buf)
				if err != nil {
					ts.Errorf("unexpected error: %s", err)
				}
				if string(buf[:n]) != test.output {
					ts.Errorf("expected %q, got %q, length %d", test.output, string(buf[:n]), n)
				}
			})
		}
	})
}

func TestShouldReturnOK(t *testing.T) {
	teardown := SetupMaster(t)
	defer teardown(t)
	n := 10
	client, err := net.Dial("tcp", fmt.Sprintf(":%d", PORT))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	for i := 0; i < n; i++ {
		client.Write([]byte("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	}

	for i := 0; i < n; i++ {
		res := resp.SimpleString{}
		if _, err := res.UnmarshalRESP(bufio.NewReader(client)); err != nil {
			t.Errorf("unexpected error: %s", err)
		}
	}
}

func TestHandshakeWithMaster(t *testing.T) {
	teardown := SetupMaster(t)
	defer teardown(t)
	replica, err := net.Dial("tcp", ":6379")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if err := utils.EstablishReplicaMaster(replica); err != nil {
		t.Error(err)
	}
}

func TestSingleReplicaPropagation(t *testing.T) {
	teardown := SetupMaster(t)
	defer teardown(t)
	replica, err := utils.ConnectReplica(fmt.Sprintf(":%d", PORT))
	if err != nil {
		t.Fatalf("Failed to connect replica: %s", err)
	}
	t.Logf("Connected replica")
	client, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", PORT), 5*time.Second)
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

	n, err = replica.Read(buf)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if string(buf[:n]) != expect {
		t.Errorf("Propogation failed, expected %q, got %q", expect, string(buf[:n]))
	}
}

func TestMultiReplicaPropagation(t *testing.T) {
	N := 4
	teardown := SetupMaster(t)
	defer teardown(t)
	replicas := make([]net.Conn, 0, 4)
	for i := 0; i < N; i++ {
		replica, err := utils.ConnectReplica(fmt.Sprintf(":%d", PORT))
		if err != nil {
			t.Fatalf("Failed to connect replica: %s", err)
		}
		replicas = append(replicas, replica)
		t.Logf("Connected replica %d", i)
	}
	buf := make([]byte, 1024)
	commands := []string{
		"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$1\r\n1\r\n",
		"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$1\r\n2\r\n",
		"*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$1\r\n3\r\n",
	}
	var (
		n   int
		err error
	)
	done := make(chan struct{}, len(commands))
	for _, c := range commands {
		go func(c string) {
			client, err := net.DialTimeout("tcp", ":6379", 5*time.Second)
			if err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			n, err = client.Write([]byte(c))
			if err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			n, err = client.Read(buf)
			if err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			if string(buf[:n]) != "+OK\r\n" {
				t.Errorf("expected +OK, got %q", string(buf[:n]))
			}
			done <- struct{}{}
		}(c)
	}
	for i := 0; i < len(commands); i++ {
		<-done
	}
	t.Logf("")
	for _, r := range replicas {
		n, err = r.Read(buf)
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

func TestReadIoTimeout(t *testing.T) {
	t.Skip()
	teardown := SetupMaster(t)
	defer teardown(t)
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
	error := resp.SimpleError{}
	reader := bufio.NewReader(bytes.NewBuffer(buff[:n]))
	if _, err = error.UnmarshalRESP(reader); err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
}

func TestPropagation(t *testing.T) {
	var (
		n   int
		err error
	)
	teardown := SetupMaster(t)
	defer teardown(t)
	teardownReplica, _ := SetupReplicaOf(t, fmt.Sprintf(":%d", PORT), 6380)
	defer teardownReplica(t)
	if err != nil {
		t.Fatalf("Failed to create replica: %s", err)
	}
	t.Logf("Connected replica")
	client, err := net.Dial("tcp", ":6379")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	buf := make([]byte, 1024)
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
		client.Write([]byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$3\r\n%s\r\n$3\r\n%s\r\n", c.key, c.val)))
	}
	t.Log("Checking replica")
	client, err = net.DialTimeout("tcp", ":6380", time.Second*5)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	time.Sleep(100 * time.Millisecond)
	for _, c := range commands {
		n, err = client.Write([]byte(fmt.Sprintf("*2\r\n$3\r\nget\r\n$3\r\n%s\r\n", c.key)))
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}

		n, err = client.Read(buf)
		if string(buf[:n]) != fmt.Sprintf("$3\r\n%s\r\n", c.val) {
			t.Errorf("expected %q, got %q", c.val, string(buf[:n]))
		}
	}
}
