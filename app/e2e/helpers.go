package e2e

import (
	"bufio"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/handlers"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

var (
	MASTER_PORT = 6379
)

func EstablishReplicaMaster(rdb *resp.Rdb, w io.WriteCloser, r *bufio.Reader) error {
	w.Write([]byte("*1\r\n$4\r\nping\r\n"))
	ok := resp.SimpleString{}
	if _, err := ok.UnmarshalRESP(r); err != nil {
		return err
	}

	if ok.S != "PONG" {
		return fmt.Errorf("expected %s, got %s", "PONG", ok.S)
	}

	w.Write([]byte("*3\r\n$8\r\nreplconf\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"))
	if _, err := ok.UnmarshalRESP(r); err != nil {
		return err
	}

	if ok.S != "OK" {
		return fmt.Errorf("expected %s, got %s", "OK", ok.S)
	}

	w.Write([]byte("*3\r\n$8\r\nreplconf\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	if _, err := ok.UnmarshalRESP(r); err != nil {
		return err
	}

	if ok.S != "OK" {
		return fmt.Errorf("expected %s, got %s", "OK", ok.S)
	}

	w.Write([]byte("*3\r\n$5\r\npsyncr\n$1\r\n?\r\n$2\r\n-1\r\n"))
	if _, err := (&resp.SimpleString{}).UnmarshalRESP(r); err != nil {
		return err
	}

	if err := rdb.UnmarshalRESP(r); err != nil {
		return err
	}

	return nil
}

func ConnectReplica(host string) (io.Writer, *bufio.Reader, *resp.Rdb, error) {
	replica, err := net.DialTimeout("tcp", host, 5*time.Second)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unexpected error: %s", err)
	}

	r := bufio.NewReader(replica)
	rdb := resp.NewRdb(&sync.Map{})
	if err := EstablishReplicaMaster(rdb, replica, r); err != nil {
		return nil, nil, nil, err
	}

	return replica, r, rdb, err
}

func SetupMaster(t testing.TB, port int) (*lib.RedisServer, *lib.Router) {
	t.Helper()
	config := lib.GetDefaultConfig()
	config.Port = port
	config.PersistenceConfig.File = ""
	config.PersistenceConfig.Dir = ""
	return setUpMaster(t, config, lib.NewRouter())
}

func SetupMasterWithReplicationHandlers(t testing.TB, port int) (*lib.RedisServer, *lib.Router) {
	t.Helper()
	config := lib.GetDefaultConfig()
	config.Port = port
	config.PersistenceConfig.File = ""
	config.PersistenceConfig.Dir = ""
	router := lib.NewRouter()
	router.RegisterHandlerFunc("ping", handlers.HandlePing)
	router.RegisterHandlerFunc("info", handlers.HandleInfo)
	router.RegisterHandlerFunc("replconf", lib.HandleReplicationConf)
	router.RegisterHandlerFunc("psync", lib.HandlePsync)
	return setUpMaster(t, config, router)
}

func setUpMaster(t testing.TB, config *lib.ServerConfig, router *lib.Router) (*lib.RedisServer, *lib.Router) {
	server, err := lib.New(config, router)
	if err != nil {
		t.Fatalf("unexpected error %s", err)
	}

	t.Cleanup(func() {
		server.Close()
	})

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		wg.Done()
		if err := server.ListenAndServe(); err != nil {
			t.Fatalf("unexpected error %s", err)
		}
	}()
	wg.Wait()

	return server, router
}

func SetupReplicaOf(t testing.TB, port int, masterAddr string) (*lib.RedisServer, *lib.Router) {
	t.Helper()
	conf := lib.GetDefaultConfig()
	conf.ReplicaOf = masterAddr
	conf.Port = port
	router := lib.NewRouter()
	replica, err := lib.New(conf, router)
	if err != nil {
		t.Fatalf("unexpected error %s", err)
	}

	t.Cleanup(func() {
		replica.Close()
	})
	errChan := make(chan error)
	go func() {
		if err := replica.ConnectMaster(); err != nil {
			errChan <- err
			return
		}
		errChan <- nil
		replica.ListenAndServe()
	}()

	if err = <-errChan; err != nil {
		t.Fatalf("unexpected error %s", err)
	}

	return replica, router
}

func TryString(a *resp.Any) ([]byte, bool) {
	switch v := a.I.(type) {
	case resp.BulkString:
		return v.S, true
	case resp.SimpleString:
		return []byte(v.S), true
	case resp.SimpleError:
		return []byte(v.E), true
	}

	return nil, false
}
