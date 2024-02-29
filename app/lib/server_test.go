package lib

import (
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/handlers"
	"net"
	"testing"
)

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
			t.Errorf("expected %v, got %v", test.expected, result)
		}
	}
}

func TestServerShouldAcceptConnection_ListenAndServe(t *testing.T) {
	server, err := New(nil)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	go server.ListenAndServe()
	defer server.Close()
	_, err = net.Dial("tcp", "localhost:6379")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestServerShouldReturnPong_ListenAndServer(t *testing.T) {
	server, err := New(nil)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	server.RegisterHandler("ping", handlers.Ping)
	go server.ListenAndServe()
	defer server.Close()
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
	server, err := New(nil)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	server.RegisterHandler("ping", handlers.Ping)
	go server.ListenAndServe()
	defer server.Close()
	t.Run("ping", func(t *testing.T) {
		for i := 0; i < 100; i++ {
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
	})
}
