package main

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	"github.com/codecrafters-io/redis-starter-go/app/lib/handlers"
	"github.com/codecrafters-io/redis-starter-go/app/lib/storage"
	"os"
	"strconv"
	"time"
)

var DefaultConfig = &lib.ServerConfig{
	Host:                   "localhost",
	Port:                   6379,
	ConnectionReadTimeout:  time.Second * 2,
	ConnectionWriteTimeout: time.Second * 2,
	ReplicationConfig: &lib.ReplicationConfig{
		ReplicationEnabled: false,
		Role:               "master",
		ConnectedSlaves:    0,
		MasterReplid:       "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		MasterReplOffset:   0,
		SecondReplOffset:   -1,
		ReplBacklogActive:  0,
		ReplBacklogSize:    1048576,
		ReplBacklogFirst:   0,
		ReplBacklogHistlen: 0,
	},
}

const HELP = `Usage: redis-starter-go [options]
	--port <port>		Port to listen on
	--help			Show this help message	
`

func main() {
	args := os.Args[1:]
	for i, v := range args {
		switch v {
		case "--help":
			fmt.Println(HELP)
			os.Exit(0)
		case "--port", "-p":
			if i+1 >= len(args) {
				fmt.Println("Invalid port")
				os.Exit(1)
			}
			port, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				fmt.Println("Invalid port")
				os.Exit(1)
			}
			DefaultConfig.Port = int(port)
		}
	}
	server, err := lib.New(DefaultConfig)

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
