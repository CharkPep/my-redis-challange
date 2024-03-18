package repl

import (
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"io"
	"sync/atomic"
)

type ReplicationConfig struct {
	Role               string
	ConnectedSlaves    atomic.Uint64
	MasterReplid       string
	MasterReplOffset   atomic.Uint64
	SecondReplOffset   atomic.Uint64
	ReplBacklogActive  int
	ReplBacklogSize    int
	ReplBacklogFirst   int
	ReplBacklogHistlen int
}

func (r *ReplicationConfig) MarshalRESP(w io.Writer) (int, error) {
	const format = `role:%s
					connected_slaves:%d
					master_replid:%s
					master_repl_offset:%d
					second_repl_offset:%d
					repl_backlog_active:%d
					repl_backlog_size:%d
					repl_backlog_first_byte_offset:%d
					repl_backlog_histlen:%d`
	return resp.BulkString{S: []byte(fmt.Sprintf(format,
		r.Role,
		r.ConnectedSlaves.Load(),
		r.MasterReplid,
		r.MasterReplOffset.Load(),
		-1,
		r.ReplBacklogActive,
		r.ReplBacklogSize,
		r.ReplBacklogFirst,
		r.ReplBacklogHistlen,
	))}.MarshalRESP(w)
}
