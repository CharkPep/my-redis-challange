package e2e

import (
	"bufio"
	"bytes"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/handlers"
	"github.com/codecrafters-io/redis-starter-go/app/lib/storage"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestServerShouldAcceptConnection(t *testing.T) {
	SetupMaster(t, MASTER_PORT)
	_, err := net.Dial("tcp", fmt.Sprintf(":%d", MASTER_PORT))
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestServerShouldReturnPong(t *testing.T) {
	_, router := SetupMaster(t, MASTER_PORT)
	router.RegisterHandlerFunc("ping", handlers.HandlePing)
	conn, err := net.Dial("tcp", fmt.Sprintf(":%d", MASTER_PORT))
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
	_, router := SetupMaster(t, MASTER_PORT)
	router.RegisterHandlerFunc("echo", handlers.HandleEcho)
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
	type tt struct {
		c resp.Array
		e resp.Any
	}

	tc := []tt{
		{
			c: resp.Array{[]resp.Marshaller{
				resp.BulkString{S: []byte("SET")},
				resp.BulkString{S: []byte("val")},
				resp.BulkString{S: []byte("key")},
			}},
			e: resp.Any{I: resp.SimpleString{S: "OK"}},
		},
	}

	_, router := SetupMaster(t, MASTER_PORT)
	router.RegisterHandlerFunc("set", handlers.HandleSet)
	client, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", MASTER_PORT), time.Second)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	t.Helper()
	r := bufio.NewReader(client)
	for i, test := range tc {
		t.Run(fmt.Sprintf("set-%d", i), func(t *testing.T) {
			test.c.MarshalRESP(client)
			res := resp.SimpleString{}
			if _, err := res.UnmarshalRESP(r); err != nil {
				t.Errorf("unexpected error %s", err)
			}

			if res != test.e.I {
				t.Errorf("expected %q, got %q", test.e.I, res)
			}
		})
	}

}

func TestShouldExpireKeys(t *testing.T) {
	var (
		SECONDS      = "EX"
		MILLISECONDS = "PX"
		TIMESTAMP    = "EXAT"
		TIMESTAMP_MS = "PXAT"
	)

	type tt struct {
		expire time.Duration
		flag   string
	}

	tc := []tt{
		{
			expire: time.Millisecond * 10,
			flag:   MILLISECONDS,
		},
		{
			expire: time.Second * 1,
			flag:   SECONDS,
		},
		{
			expire: time.Now().Add(time.Millisecond + 10).Sub(time.Now()),
			flag:   TIMESTAMP,
		},
		{
			expire: time.Now().Add(time.Millisecond + 10).Sub(time.Now()),
			flag:   TIMESTAMP_MS,
		},
	}

	_, router := SetupMaster(t, MASTER_PORT)
	router.RegisterHandlerFunc("set", handlers.HandleSet)
	router.RegisterHandlerFunc("get", handlers.HandleGet)
	for i, test := range tc {
		test := test
		t.Run(fmt.Sprintf("expire-%d-%s", i, test.expire), func(t *testing.T) {
			//if test.flag == SECONDS {
			//	t.Skipf()
			//}
			key := fmt.Sprint(time.Now().UnixNano())
			t.Parallel()
			client, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", MASTER_PORT), time.Second)
			if err != nil {
				t.Fatal(err)
			}

			r := bufio.NewReader(client)
			c := resp.Array{[]resp.Marshaller{
				resp.BulkString{S: []byte("SET")},
				resp.BulkString{S: []byte(key)},
				resp.BulkString{S: []byte("foo")},
				resp.BulkString{S: []byte(test.flag)},
			}}

			switch test.flag {
			case SECONDS:
				c.A = append(c.A,
					resp.BulkString{S: []byte(fmt.Sprint(test.expire.Seconds()))},
				)
			case MILLISECONDS:
				c.A = append(c.A,
					resp.BulkString{S: []byte(fmt.Sprint(test.expire.Milliseconds()))},
				)
			case TIMESTAMP:
				c.A = append(c.A,
					resp.SimpleInt{time.Now().Add(test.expire).Unix()},
				)
			case TIMESTAMP_MS:
				c.A = append(c.A,
					resp.SimpleInt{time.Now().Add(test.expire).UnixMilli()},
				)
			default:
				t.Logf("flag is not found %s", test.flag)
			}

			if _, err := c.MarshalRESP(client); err != nil {
				t.Fatal(err)
			}

			res := resp.Any{}
			if _, err = res.UnmarshalRESP(r); err != nil {
				t.Errorf("Failed to unmarshal: %s", err)
			}

			if res != (resp.Any{I: resp.SimpleString{S: "OK"}}) {
				t.Errorf("expected %v, got %v", resp.Any{I: resp.SimpleString{S: "OK"}}, res)
			}

			t.Logf("Sleep for %s", test.expire.String())
			time.Sleep(test.expire + time.Millisecond*10)
			(resp.Array{[]resp.Marshaller{
				resp.BulkString{S: []byte("GET")},
				resp.BulkString{S: []byte(key)},
			}}).MarshalRESP(client)

			res = resp.Any{}

			if _, err = res.UnmarshalRESP(r); err != nil {
				t.Error(err)
			}

			resS, ok := res.I.(resp.BulkString)
			if !ok {
				t.Errorf("expected %T, got %T", resp.BulkString{}, res.I)
			}

			if resS.S != nil {
				t.Errorf("expeted nil, got %s", resS.S)
			}

			if !resS.EncodeNil {
				t.Errorf("expected encode nil to be true")
			}

			t.Logf("Done")
		})
	}
}

func TestShouldReturnType(t *testing.T) {
	type tt struct {
		c resp.Array
		g resp.Array
		t string
		k string
	}

	getStrT := func(k string) resp.Array {
		return resp.Array{A: []resp.Marshaller{
			resp.BulkString{S: []byte("TYPE")},
			resp.BulkString{S: []byte(k)},
		}}
	}

	tc := []tt{
		{
			c: resp.Array{A: []resp.Marshaller{
				resp.BulkString{S: []byte("SET")},
				resp.BulkString{S: []byte("k1")},
				resp.BulkString{S: []byte("v1")},
			}},
			g: getStrT("k1"),
			t: storage.STRINGS.String(),
			k: "k1",
		},
		//{
		//	c: resp.Array{A: []resp.Marshaller{
		//		resp.BulkString{S: []byte("SET")},
		//		resp.BulkString{S: []byte("k1")},
		//		resp.BulkString{S: []byte("v1")},
		//		resp.BulkString{S: []byte("DELETE")},
		//		resp.BulkString{S: []byte("k1")},
		//	}},
		//},
	}

	_, router := SetupMaster(t, MASTER_PORT)
	router.RegisterHandlerFunc("set", handlers.HandleSet)
	router.RegisterHandlerFunc("type", handlers.HandleType)
	for i, test := range tc {
		t.Run(fmt.Sprintf("type-%d", i), func(t *testing.T) {
			t.Parallel()
			client, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", MASTER_PORT), time.Second)
			if err != nil {
				t.Error(err)
			}

			r := bufio.NewReader(client)
			if _, err := test.c.MarshalRESP(client); err != nil {
				t.Error(err)
			}

			//	skip response
			if _, err := (&resp.Any{}).UnmarshalRESP(r); err != nil {
				t.Error(err)
			}

			if _, err := test.g.MarshalRESP(client); err != nil {
				t.Error(err)
			}

			tKey := resp.Any{}
			if _, err := tKey.UnmarshalRESP(r); err != nil {
				t.Error(err)
			}

			t.Logf("%s", reflect.TypeOf(tKey.I))
			//if tKey.S != test.t {
			//	t.Errorf("expected type %s, got %s", test.t, tKey.S)
			//}

		})
	}
}

func TestHandleStreamXAdd(t *testing.T) {
	type tt struct {
		c   resp.Array
		e   resp.Any
		now bool
	}

	ts := []tt{
		{
			c: resp.Array{A: []resp.Marshaller{
				resp.BulkString{S: []byte("XADD")},
				resp.BulkString{S: []byte("stream")},
				resp.BulkString{S: []byte("0-1")},
				resp.BulkString{S: []byte("filed")},
				resp.BulkString{S: []byte("value")},
			}},
			e: resp.Any{I: resp.BulkString{S: []byte("0-1")}},
		},
	}
	_, router := SetupMaster(t, MASTER_PORT)
	router.RegisterHandlerFunc("xadd", handlers.HandleXAdd)
	for i, test := range ts {
		test := test
		t.Run(fmt.Sprintf("xadd-%d", i), func(t *testing.T) {
			client, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", MASTER_PORT), time.Second)
			if err != nil {
				t.Error(err)
			}
			defer client.Close()
			r := bufio.NewReader(client)

			if _, err := test.c.MarshalRESP(client); err != nil {
				t.Error(err)
			}

			res := resp.Any{}
			if _, err := res.UnmarshalRESP(r); err != nil {
				t.Error(err)
			}

			if !reflect.DeepEqual(res.I, test.e.I) {
				t.Errorf("expected %s, got %s", test.e.I, res.I)
			}
		})
	}

}
