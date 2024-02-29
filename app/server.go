package main

import (
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	"github.com/codecrafters-io/redis-starter-go/app/lib/handlers"
)

func main() {
	server, err := lib.New(nil)
	if err != nil {
		panic(err)
	}
	// As mentioned, though stupid af, in https://redis.io/commands/command/ the command is case-insensitive
	// so we register the handler for both "ping" and "PING"
	server.RegisterHandler("ping", handlers.Ping)
	server.RegisterHandler("PING", handlers.Ping)
	server.RegisterHandler("echo", handlers.Echo)
	server.RegisterHandler("ECHO", handlers.Echo)
	defer server.Close()
	server.ListenAndServe()
}
