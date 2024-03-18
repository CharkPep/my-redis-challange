package main

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib"
	"github.com/codecrafters-io/redis-starter-go/app/lib/handlers"
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

func RegisterHandlers(router *lib.Router) {
	//  As mentioned, though stupid af, in https://redis.io/commands/command/ the command is case-insensitive
	// so we register the handler for both "ping" and "PING"
	pingHandler := handlers.PingHandler{}
	echoHandler := handlers.EchoHandler{}
	stringsStore := storage.New(nil)
	setHandler := handlers.StringsSetHandler{Storage: stringsStore}
	replicatedSet := lib.NewReplicationWrapper(&setHandler)
	getHandler := handlers.StringsGetHandler{Storage: stringsStore}
	infoHandler := lib.InfoHandler{}
	replConfHandler := lib.ReplConfHandler{}
	psyncHandler := lib.PsyncHandler{}
	waitHander := lib.WaitHandler{}
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
