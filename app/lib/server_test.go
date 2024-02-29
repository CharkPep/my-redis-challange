package lib

import (
	"bytes"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/handlers"
	"net"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	server, err := New(nil)
	if err != nil {
		panic(err)
	}
	server.RegisterHandler("ping", handlers.Ping)
	server.RegisterHandler("echo", handlers.Echo)
	go server.ListenAndServe()
	m.Run()
	server.Close()
	os.Exit(0)
}

func TestServer_getCommand(t *testing.T) {
	type testCases struct {
		input    *resp.AnyResp
		expected string
	}
	tests := []testCases{
		{
			input:    &resp.AnyResp{I: resp.SimpleString{"hello"}},
			expected: "hello",
		},
		{
			input:    &resp.AnyResp{I: resp.BulkString{S: []byte("hello")}},
			expected: "hello",
		},
		{
			input: &resp.AnyResp{
				I: resp.RespArray{
					A: []resp.RespMarshaler{
						resp.BulkString{[]byte("foo"), false},
						resp.SimpleString{"bar"},
						resp.SimpleInt{-1},
					},
				},
			},
			expected: "foo",
		},
	}
	for _, test := range tests {
		result, _ := getCommand(test.input)
		if result != test.expected {
			t.Fatalf("expected %v, got %v", test.expected, result)
		}
	}
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
		t.Errorf("expected +PONG, got %s, length %d", string(buf[:n]), n)
	}
}

func TestServerShouldReturnPongConcurrently_ListenAndServer(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Run(fmt.Sprintf("ping:%d", i), func(t *testing.T) {
			t.Parallel()
			conn, err := net.Dial("tcp", "localhost:6379")
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			time.Sleep(10 * time.Millisecond)
			_, err = conn.Write([]byte("*1\r\n$4\r\nping\r\n"))
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if string(buf[:n]) != "+PONG\r\n" {
				t.Fatalf("expected +PONG, got %s, length %d", string(buf[:n]), n)
			}
		})
	}
}

func TestServerShouldReturnEcho_ListenAndServer(t *testing.T) {
	type testCase struct {
		args   resp.RespMarshaler
		output string
	}
	tests := []testCase{
		{
			args:   resp.RespArray{A: []resp.RespMarshaler{resp.BulkString{S: []byte("echo")}, resp.BulkString{S: []byte("foo")}}},
			output: "*1\r\n$3\r\nfoo\r\n",
		},
		{
			args: resp.RespArray{A: []resp.RespMarshaler{resp.BulkString{S: []byte("echo")},
				resp.BulkString{S: []byte("foo")}, resp.BulkString{S: []byte("bar")}}},
			output: "*1\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("echo:%d", i), func(t *testing.T) {
			t.Parallel()
			conn, err := net.Dial("tcp", "localhost:6379")
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			buff := bytes.NewBuffer([]byte{})
			err = test.args.MarshalRESP(buff)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if _, err = conn.Write(buff.Bytes()); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			t.Logf("expected %s, got %s, length %d", test.output, string(buf[:n]), n)
			if string(buf[:n]) != test.output {
				t.Fatalf("expected %s, got %s, length %d", test.output, string(buf[:n]), n)
			}
		})
	}
}
