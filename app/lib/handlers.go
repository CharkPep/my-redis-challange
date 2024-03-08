package lib

import (
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/repl"
	"net"
	"strconv"
)

func (s *Server) HandleInfo(ctx context.Context, args *resp.Array) (interface{}, error) {
	if len(args.A) < 1 {
		return nil, fmt.Errorf("ERR wrong number of arguments")
	}
	section, ok := args.A[0].(resp.BulkString)
	if !ok {
		return nil, fmt.Errorf("ERR invalid section type, expected string, got %T", (args.A)[0])
	}
	switch string(section.S) {
	case "replication":
		return s.config.ReplicationConfig, nil
	default:
		return nil, fmt.Errorf("ERR invalid section: %s", section.S)
	}
}

func (s *Server) HandleReplConf(ctx context.Context, args *resp.Array) (interface{}, error) {
	switch v := args.A[0].(type) {
	case resp.BulkString:
		switch string(v.S) {
		case "listening-port":
			if len(args.A) < 2 {
				return nil, fmt.Errorf("ERR wrong number of arguments for command")
			}
			port, ok := args.A[1].(resp.BulkString)
			if !ok {
				return nil, fmt.Errorf("ERR invalid port type, expected string, got %T", args.A[1])
			}
			portNum, err := strconv.ParseInt(string(port.S), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("ERR invalid port: %s", err)
			}
			conn, ok := ctx.Value("conn").(net.Conn).(*net.TCPConn)
			if !ok {
				return nil, fmt.Errorf("ERR invalid connection type, expected *net.TCPConn, got %T", conn)
			}
			s.replicas[int(portNum)] = repl.NewReplica(conn.RemoteAddr().String(), int(portNum))
			return "OK", nil
		case "capa":
			return "OK", nil
		}
	}
	return nil, fmt.Errorf("ERR invalid command")
}

func (s *Server) HandlePsync(ctx context.Context, args *resp.Array) (interface{}, error) {
	return fmt.Sprintf("FULLRESYNC %s 0", s.config.ReplicationConfig.MasterReplid), nil
}
