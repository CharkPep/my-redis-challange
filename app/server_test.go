package main

import (
	"bytes"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/repl"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
	"net"
	"os"
	"regexp"
	"testing"
)

func TestMain(m *testing.M) {
	replicas := repl.NewReplicaManager()
	server, err := lib.New(nil, replicas)
	if err != nil {
		panic(err)
	}

	RegisterHandlers(server, replicas)
	go server.ListenAndServe()
	code := m.Run()
	err = server.Close()
	if err != nil {
		fmt.Println("Failed to terminate")
		os.Exit(1)
	}
	fmt.Println("Terminated successfully")
	os.Exit(code)
}

func TestServerShouldAcceptConnection_ListenAndServe(t *testing.T) {
	_, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestServerShouldReturnPong_ListenAndServer(t *testing.T) {
	conn, err := net.Dial("tcp", "localhost:6379")
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

func TestServerShouldReturnEcho_ListenAndServer(t *testing.T) {
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

	for i, test := range tests {
		t.Run(fmt.Sprintf("echo:%d", i), func(ts *testing.T) {
			test := test
			ts.Parallel()
			conn, err := net.Dial("tcp", "localhost:6379")
			if err != nil {
				ts.Errorf("unexpected error: %s", err)
			}
			buff := bytes.NewBuffer([]byte{})
			err = test.args.MarshalRESP(buff)
			t.Logf("%q", buff.String())
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
}

func TestServer_HandleInfoShouldRespond(t *testing.T) {
	conn, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	conn.Write([]byte("*2\r\n$4\r\ninfo\r\n$11\r\nreplication\r\n"))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if n == 0 {
		t.Errorf("expected something, got nothing")
	}
}

// Does not care just for testing
func BenchmarkRandomStringGenerator(b *testing.B) {
	for i := 0; i < b.N; i++ {
		w := bytes.NewBuffer(make([]byte, 0, 10))
		utils.RandomAlphanumericString(w, 10)
		if len(w.Bytes()) != 10 {
			b.Errorf("expected 10, got %d", len(w.Bytes()))
		}

		for _, c := range w.Bytes() {
			if (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
				b.Errorf("expected alphanumeric, got %q", c)
			}
		}
	}
}

func TestHandshakeFlow(t *testing.T) {
	replica, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	replica.Write([]byte("*1\r\n$4\r\nping\r\n"))
	buf := make([]byte, 1024)
	n, err := replica.Read(buf)
	if string(buf[:n]) != "+PONG\r\n" {
		t.Errorf("expected +PONG, got %q", string(buf[:n]))
	}

	replica.Write([]byte("*3\r\n$8\r\nreplconf\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))
	n, err = replica.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		t.Errorf("expected +OK on replconf port, got %q", string(buf[:n]))
	}

	replica.Write([]byte("*3\r\n$8\r\nreplconf\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	n, err = replica.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		t.Errorf("expected +OK on replconf capa, got %q", string(buf[:n]))
	}

	replica.Write([]byte("*3\r\n$5\r\npsyncr\n$1\r\n?\r\n$2\r\n-1\r\n"))
	buf = make([]byte, 1024)
	n, err = replica.Read(buf)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	t.Logf("Psyncr response: %v", buf[:n])
	t.Logf("Got %d bytes", n)
	match, err := regexp.Match("\\+FULLRESYNC \\w+.*", buf[:n])
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	if !match {
		t.Errorf("expected FULLRESYNC, got %q", string(buf[:n]))
	}

	client, err := net.Dial("tcp", "localhost:6379")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	expect := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	client.Write([]byte(expect))
	n, err = client.Read(buf)
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
