package main

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	"github.com/codecrafters-io/redis-starter-go/app/lib/handlers"
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
	server, err := lib.New(config)

	if err != nil {
		panic(err)
	}
	// As mentioned, though stupid af, in https://redis.io/commands/command/ the command is case-insensitive
	// so we register the handler for both "ping" and "PING"
	server.RegisterHandler("ping", handlers.Ping)
	server.RegisterHandler("PING", handlers.Ping)
	server.RegisterHandler("echo", handlers.Echo)
	server.RegisterHandler("ECHO", handlers.Echo)
	stringsStore := storage.New(nil)
	stringsHandler := handlers.StringHandler{
		Storage: stringsStore,
	}
	server.RegisterHandler("set", stringsHandler.HandleSet)
	server.RegisterHandler("SET", stringsHandler.HandleSet)
	server.RegisterHandler("get", stringsHandler.HandleGet)
	server.RegisterHandler("GET", stringsHandler.HandleGet)
	server.RegisterHandler("info", server.HandleInfo)
	server.RegisterHandler("INFO", server.HandleInfo)
	defer server.Close()
	server.ListenAndServe()
}
