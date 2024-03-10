package main

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	"github.com/codecrafters-io/redis-starter-go/app/lib/handlers"
	"github.com/codecrafters-io/redis-starter-go/app/lib/middleware"
	"github.com/codecrafters-io/redis-starter-go/app/lib/repl"
	"github.com/codecrafters-io/redis-starter-go/app/lib/storage"
	"log"
	"os"
	"strconv"
)

const HELP = `
Usage: <build> [options]
--port <port>			Port to listen on
--help				Show this help message	
--replicaof <host> <port>	Make the server a slave of another instance
`

func RegisterHandlers(server *lib.Server, replicas *repl.ReplicaManager) {
	// As mentioned, though stupid af, in https://redis.io/commands/command/ the command is case-insensitive
	// so we register the handler for both "ping" and "PING"
	pingHandler := handlers.PingHandler{}
	echoHandler := handlers.EchoHandler{}
	stringsStore := storage.New(nil)
	setHandler := handlers.StringsSetHandler{Storage: stringsStore}
	replicatedSet := middleware.NewReplicationWrapper(&setHandler, replicas)
	getHandler := handlers.StringsGetHandler{Storage: stringsStore}
	infoHandler := lib.InfoHandler{server}
	replConfHandler := lib.ReplConfHandler{server}
	psyncHandler := lib.PsyncHandler{server}
	server.RegisterHandler("ping", pingHandler)
	server.RegisterHandler("PING", pingHandler)
	server.RegisterHandler("echo", echoHandler)
	server.RegisterHandler("ECHO", echoHandler)
	server.RegisterHandler("set", replicatedSet)
	server.RegisterHandler("SET", replicatedSet)
	server.RegisterHandler("get", getHandler)
	server.RegisterHandler("GET", getHandler)
	server.RegisterHandler("info", infoHandler)
	server.RegisterHandler("INFO", infoHandler)
	server.RegisterHandler("replconf", replConfHandler)
	server.RegisterHandler("REPLCONF", replConfHandler)
	server.RegisterHandler("psync", psyncHandler)
	server.RegisterHandler("PSYNC", psyncHandler)
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
			config.ReplicationConfig.Role = "slave"
			port, err := strconv.ParseInt(args[i+2], 10, 64)
			if err != nil {
				log.Fatal("Invalid port")
			}
			repl, err := repl.NewReplicaOf(args[i+1], int(port), config.Port)
			if err != nil {
				log.Fatal("Failed to handshake with master: ", err)
			}
			config.ReplicaOf = repl
		}
	}
	replicas := repl.NewReplicaManager()
	server, err := lib.New(config, replicas)
	if err != nil {
		panic(err)
	}
	RegisterHandlers(server, replicas)
	defer server.Close()
	server.ListenAndServe()
}
