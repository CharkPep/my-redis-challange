package lib

import (
	"context"
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"time"
)

func HandlePsync(ctx context.Context, req *RESPRequest) (interface{}, error) {
	req.Logger.Printf("Sending resync to %s", req.RemoteAddr)
	if _, err := (resp.SimpleString{S: fmt.Sprintf("FULLRESYNC %s %d", req.s.config.ReplicationConfig.MasterReplid,
		req.s.config.ReplicationConfig.MasterReplOffset.Load())}).MarshalRESP(req.W); err != nil {
		return nil, err
	}

	req.Logger.Printf("Sending full resync to %s", req.RemoteAddr)
	if _, err := req.s.rdb.FullResync(req.W); err != nil {
		return nil, err
	}
	if err := req.conn.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}
	if err := req.conn.SetWriteDeadline(time.Time{}); err != nil {
		return nil, err
	}

	req.s.config.ReplicationConfig.ConnectedSlaves.Add(1)
	req.Logger.Printf("Slave connected: %s", req.RemoteAddr)
	req.s.logger.SetPrefix(fmt.Sprintf("master[%d]", req.s.config.ReplicationConfig.ConnectedSlaves.Load()))
	<-ctx.Done()
	req.s.config.ReplicationConfig.ConnectedSlaves.Swap(uint64(req.s.config.ReplicationConfig.ConnectedSlaves.Load() - 1))
	return nil, nil
}
