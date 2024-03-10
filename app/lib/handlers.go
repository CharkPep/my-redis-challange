package lib

import (
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/lib/repl"
	"net"
)

type InfoHandler struct {
	S *Server
}

func (i InfoHandler) HandleResp(ctx context.Context, args *resp.Array) (interface{}, error) {
	if len(args.A) < 1 {
		return nil, fmt.Errorf("ERR wrong number of arguments")
	}
	section, ok := args.A[0].(resp.BulkString)
	if !ok {
		return nil, fmt.Errorf("ERR invalid section type, expected string, got %T", (args.A)[0])
	}
	switch string(section.S) {
	case "replication":
		return i.S.config.ReplicationConfig, nil
	default:
		return nil, fmt.Errorf("ERR invalid section: %s", section.S)
	}
}

type ReplConfHandler struct {
	S *Server
}

func (c ReplConfHandler) HandleResp(ctx context.Context, args *resp.Array) (interface{}, error) {
	switch v := args.A[0].(type) {
	case resp.BulkString:
		switch string(v.S) {
		case "listening-port":
			if len(args.A) < 2 {
				return nil, fmt.Errorf("ERR wrong number of arguments for command")
			}
			ctxMap, ok := ctx.Value("ctx").(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("ERR invalid context, expected map[string]interface{}, got %T", ctx.Value("ctx"))
			}
			conn, ok := ctxMap["conn"].(*net.TCPConn)
			if !ok {
				return nil, fmt.Errorf("ERR invalid connection type, expected *net.TCPConn, got %T", ctxMap["conn"])
			}
			c.S.replicas = append(c.S.replicas, repl.NewReplica(conn))
			return "OK", nil
		case "capa":
			return "OK", nil
		}
	}
	return nil, fmt.Errorf("ERR invalid command")
}

type PsyncHandler struct {
	S *Server
}

func (p PsyncHandler) HandleResp(ctx context.Context, args *resp.Array) (interface{}, error) {
	ctxMap := ctx.Value("ctx").(map[string]interface{})
	conn, ok := ctxMap["conn"].(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("ERR invalid connection type, expected *net.TCPConn, got %T", conn)
	}

	if _, err := conn.Write([]byte(fmt.Sprintf("+FULLRESYNC %s %d\r\n", p.S.config.ReplicationConfig.MasterReplid, p.S.config.ReplicationConfig.MasterReplOffset))); err != nil {
		return nil, err
	}

	_, err := conn.Write([]byte(fmt.Sprintf("$%d\r\n%s", len(rdb.GetEmpty()), rdb.GetEmpty())))
	ctxMap["encode"] = false
	return nil, err
}
