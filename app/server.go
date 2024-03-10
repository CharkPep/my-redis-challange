package main

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	"github.com/codecrafters-io/redis-starter-go/app/lib/handlers"
	"github.com/codecrafters-io/redis-starter-go/app/lib/middleware"
	"github.com/codecrafters-io/redis-starter-go/app/lib/repl"
	"github.com/codecrafters-io/redis-starter-go/app/lib/storage"
	"os"
	"strconv"
)

const HELP = `
Usage: <build> [options]
--port <port>			Port to listen on
--help				Show this help message	
--replicaof <host> <port>	Make the server a slave of another instance
`

func main() {
	config := lib.GetDefaultConfig()
	args := os.Args[1:]
	for i, v := range args {
		switch v {
		case "--help":
			fmt.Printf(HELP)
			os.Exit(0)
		case "--port", "-p":
			if i+1 >= len(args) {
				fmt.Printf("Invalid port")
				os.Exit(1)
			}
			port, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				fmt.Println("Invalid port")
				os.Exit(1)
			}
			config.Port = int(port)
		case "--replicaof":
			if i+2 >= len(args) {
				fmt.Println("Invalid replicaof")
				os.Exit(1)
			}
			config.ReplicationConfig.Role = "slave"
			port, err := strconv.ParseInt(args[i+2], 10, 64)
			if err != nil {
				fmt.Println("Invalid port")
				os.Exit(1)
			}
			repl, err := repl.NewReplicaOf(args[i+1], int(port), config.Port)
			if err != nil {
				fmt.Println("Failed to handshake with master: ", err)
				os.Exit(1)
			}
			config.ReplicaOf = repl
		}
	}
	var replicas []*repl.Replica
	server, err := lib.New(config, replicas)

	if err != nil {
		panic(err)
	}
	// As mentioned, though stupid af, in https://redis.io/commands/command/ the command is case-insensitive
	// so we register the handler for both "ping" and "PING"
	pingHandler := handlers.PingHandler{}
	echoHandler := handlers.EchoHandler{}
	stringsStore := storage.New(nil)
	setHandler := handlers.StringsSetHandler{Storage: stringsStore}
	setReplMiddleware := middleware.NewReplSet(&setHandler, replicas)
	getHandler := handlers.StringsGetHandler{Storage: stringsStore}
	infoHandler := lib.InfoHandler{server}
	replConfHandler := lib.ReplConfHandler{server}
	psyncHandler := lib.PsyncHandler{server}
	server.RegisterHandler("ping", pingHandler)
	server.RegisterHandler("PING", pingHandler)
	server.RegisterHandler("echo", echoHandler)
	server.RegisterHandler("ECHO", echoHandler)
	server.RegisterReplicatedCommand("set", setReplMiddleware)
	server.RegisterReplicatedCommand("SET", setReplMiddleware)
	server.RegisterHandler("get", getHandler)
	server.RegisterHandler("GET", getHandler)
	server.RegisterHandler("info", infoHandler)
	server.RegisterHandler("INFO", infoHandler)
	server.RegisterHandler("replconf", replConfHandler)
	server.RegisterHandler("REPLCONF", replConfHandler)
	server.RegisterHandler("psync", psyncHandler)
	server.RegisterHandler("PSYNC", psyncHandler)

	defer server.Close()
	server.ListenAndServe()
}
