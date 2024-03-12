package lib

import (
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"github.com/codecrafters-io/redis-starter-go/app/lib/persistence"
	"github.com/codecrafters-io/redis-starter-go/app/lib/repl"
	"io"
	"log"
	"net"
	"time"
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
			c.S.replicas.AddReplica(repl.NewReplica(conn))
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
	req := ctx.Value("ctx").(map[string]interface{})
	conn, ok := req["conn"].(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("ERR invalid connection type, expected *net.TCPConn, got %T", conn)
	}

	resync := fmt.Sprintf("+FULLRESYNC %s %d\r\n", p.S.config.ReplicationConfig.MasterReplid, p.S.config.ReplicationConfig.MasterReplOffset)
	if _, err := conn.Write([]byte(resync)); err != nil {
		return nil, err
	}

	if _, err := conn.Write([]byte(fmt.Sprintf("$%d\r\n%s", len(persistence.GetEmpty()), persistence.GetEmpty()))); err != nil {
		return nil, err
	}

	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		log.Printf("Replica read timeout change ended in unexpected error: %s", err)
		return nil, err
	}
	p.S.config.ReplicationConfig.ConnectedSlaves += 1
	log.Printf("Replica %s connected", conn.RemoteAddr())
	for {
		select {
		case <-ctx.Done():
			p.S.config.ReplicationConfig.ConnectedSlaves -= 1
			log.Printf("Recieved Done, disconnecting replica %d: %s", conn.RemoteAddr(), ctx.Err())
			req["encode"] = false
			return nil, nil
		default:
			buf := make([]byte, 1024)
			log.Println("Listening for propagation")
			n, err := conn.Read(buf)
			if err == io.EOF {
				log.Println("Master disconnected, received EOF")
				return nil, nil
			}
			if err != nil {
				log.Printf("Unexpected error %s", err)
				return nil, err
			}
			log.Printf("Recieved from master: %s", buf[:n])
		}
	}
	//<-ctx.Done()
	//return nil, nil
}
