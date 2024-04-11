package main

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	"github.com/codecrafters-io/redis-starter-go/app/lib/handlers"
	"log"
	"os"
	"strconv"
)

const HELP = `
Usage: <build> [options]
--port <port>			Port to listen on
--help				Show this help message	
--replicaof <host> <port>	Make the server a replication of another instance
--dir <directory>	Set rdb directory
--dbfilename		Set rdb file name, combined with "dir" option sets path to rdb file
`

func RegisterHandlers(router *lib.Router) {
	router.RegisterHandler("set", lib.ReplWrapper{Next: lib.HandleFunc(handlers.HandleSet)})
	router.RegisterHandler("get", lib.HandleFunc(handlers.HandleGet))
	router.RegisterHandler("keys", lib.HandleFunc(handlers.HandleKeys))
	router.RegisterHandlerFunc("ping", handlers.HandlePing)
	router.RegisterHandlerFunc("echo", handlers.HandleEcho)
	router.RegisterHandlerFunc("info", handlers.HandleInfo)
	router.RegisterHandlerFunc("replconf", lib.HandleReplicationConf)
	router.RegisterHandlerFunc("psync", lib.HandlePsync)
	router.RegisterHandlerFunc("wait", lib.HandleWait)
	router.RegisterHandlerFunc("config", lib.HandleConfig)
	router.RegisterHandlerFunc("select", lib.HandleSelect)
	router.RegisterHandlerFunc("type", handlers.HandleType)
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
