package main

import (
	"bufio"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	"github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/handlers"
	"github.com/codecrafters-io/redis-starter-go/app/lib/storage"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
)

const HELP = `
Usage: <build> [options]
--port <port>			Port to listen on
--help				Show this help message	
--replicaof <host> <port>	Make the server a slave of another instance
`

var (
	stringsStorage = storage.New(nil)
)

func RegisterHandlers(router *lib.Router) {
	//  As mentioned, though stupid af, in https://redis.io/commands/command/ the command is case-insensitive
	// so we register the handler for both "ping" and "PING"
	pingHandler := handlers.PingHandler{}
	echoHandler := handlers.EchoHandler{}
	setHandler := handlers.StringsSetHandler{Storage: stringsStorage}
	replicatedSet := lib.NewReplicationWrapper(&setHandler)
	getHandler := handlers.StringsGetHandler{Storage: stringsStorage}
	keysHandler := handlers.KeysHandler{Storage: stringsStorage}
	infoHandler := lib.InfoHandler{}
	replConfHandler := lib.ReplConfHandler{}
	psyncHandler := lib.PsyncHandler{}
	waitHander := lib.WaitHandler{}
	configHandler := lib.ConfigHandler{}
	//pconfHanlder := handlers.Replconf{}
	router.RegisterHandler("ping", pingHandler)
	router.RegisterHandler("PING", pingHandler)
	router.RegisterHandler("echo", echoHandler)
	router.RegisterHandler("ECHO", echoHandler)
	router.RegisterHandler("set", replicatedSet)
	router.RegisterHandler("SET", replicatedSet)
	router.RegisterHandler("get", getHandler)
	router.RegisterHandler("GET", getHandler)
	router.RegisterHandler("info", infoHandler)
	router.RegisterHandler("INFO", infoHandler)
	router.RegisterHandler("replconf", replConfHandler)
	router.RegisterHandler("REPLCONF", replConfHandler)
	router.RegisterHandler("psync", psyncHandler)
	router.RegisterHandler("PSYNC", psyncHandler)
	router.RegisterHandler("wait", waitHander)
	router.RegisterHandler("WAIT", waitHander)
	router.RegisterHandler("config", configHandler)
	router.RegisterHandler("CONFIG", configHandler)
	router.RegisterHandler("keys", keysHandler)
}
func main() {
	log.SetPrefix("redis-server:")
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)

	config := lib.GetDefaultConfig()
	args := os.Args[1:]
	for i, v := range args {
		switch v {
		case "--help":
			fmt.Printf(HELP)
			os.Exit(0)
		case "--port", "-p":
			if i+1 >= len(args) {
				log.Fatal("Invalid port")
			}
			port, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				log.Fatal("Invalid port")
			}
			config.Port = int(port)
		case "--replicaof":
			if i+2 >= len(args) {
				log.Fatal("Invalid replicaof")
			}
			port, err := strconv.ParseInt(args[i+2], 10, 64)
			if err != nil {
				log.Fatal("Invalid port")
			}
			if err != nil {
				log.Fatal("Failed to handshake with master: ", err)
			}
			config.ReplicaOf = args[i+1] + ":" + strconv.Itoa(int(port))
		case "--dir":
			if i+1 >= len(args) {
				log.Fatal("Invalid replicaof")
			}
			config.PersistenceConfig.Dir = args[i+1]
		case "--dbfilename":
			if i+1 >= len(args) {
				log.Fatal("Invalid replicaof")
			}
			config.PersistenceConfig.File = args[i+1]
		}
	}

	if config.PersistenceConfig.Dir != "" && config.PersistenceConfig.File != "" {
		absp, err := filepath.Abs(path.Join(config.PersistenceConfig.Dir, config.PersistenceConfig.File))
		if err != nil {
			os.Exit(1)
		}

		rdbf, err := os.Open(absp)
		if err != nil {
			log.Printf("Failed to open rdb file: %s, creating new one\n", err)
			f, err := os.Create(absp)
			if err != nil {
				log.Panicf("Failed to create rdb file: %s", err)
			}

			if _, err = encoding.NewRdb().MarshalRESP(f); err != nil {
				log.Fatal(err)
			}

			log.Printf("Created new rdb file: %s", absp)
		} else {
			r := bufio.NewReader(rdbf)
			rdb := encoding.NewRdb()
			if _, err := rdb.Load(r); err != nil {
				fmt.Printf("Failed to unmarshal rdb: %s", err)
				os.Exit(1)
			}

			rdb.Apply(stringsStorage)
		}

	}

	router := lib.NewRouter()
	RegisterHandlers(router)
	server, err := lib.New(config, router)
	if err != nil {
		panic(err)
	}
	defer server.Close()
	done := make(chan struct{})
	go func() {
		server.ConnectMaster()
		if err = server.ListenAndServe(); err != nil {
			fmt.Printf("Error while listening for port %d: %s", config.Port, err)
		}
		done <- struct{}{}
	}()
	<-done

}
