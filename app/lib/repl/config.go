package repl

import (
	"fmt"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"io"
)

type ReplicationConfig struct {
	Role               string
	ConnectedSlaves    int
	MasterReplid       string
	MasterReplOffset   int
	SecondReplOffset   int
	ReplBacklogActive  int
	ReplBacklogSize    int
	ReplBacklogFirst   int
	ReplBacklogHistlen int
}

func (r *ReplicationConfig) MarshalRESP(w io.Writer) error {
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
		r.ConnectedSlaves,
		r.MasterReplid,
		r.MasterReplOffset,
		-1,
		r.ReplBacklogActive,
		r.ReplBacklogSize,
		r.ReplBacklogFirst,
		r.ReplBacklogHistlen,
	))}.MarshalRESP(w)
}
