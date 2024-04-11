package lib

import (
	"github.com/codecrafters-io/redis-starter-go/app/lib/persistence"
	"github.com/codecrafters-io/redis-starter-go/app/lib/replication"
	"sync/atomic"
	"time"
)

type ServerConfig struct {
	Host                   string
	Port                   int
	ConnectionReadTimeout  time.Duration
	ConnectionWriteTimeout time.Duration
	ReplicaOf              string
	ReplicationConfig      *replication.ReplicationConfig
	PersistenceConfig      *persistence.Config
}

func GetDefaultConfig() *ServerConfig {
	return &ServerConfig{
		Host:                   "localhost",
		Port:                   6379,
		ConnectionReadTimeout:  time.Second * 2,
		ConnectionWriteTimeout: time.Second * 2,
		ReplicationConfig: &replication.ReplicationConfig{
			Role:               "master",
			MasterReplOffset:   atomic.Uint64{},
			SecondReplOffset:   atomic.Uint64{},
			ConnectedSlaves:    atomic.Uint64{},
			ReplBacklogActive:  0,
			ReplBacklogSize:    1048576,
			ReplBacklogFirst:   0,
			ReplBacklogHistlen: 0,
		},
		PersistenceConfig: &persistence.Config{
			Dir:  "",
			File: "",
		},
	}
}

var defaultConfig = &ServerConfig{
	Host:                   "localhost",
	Port:                   6379,
	ConnectionReadTimeout:  time.Second * 2,
	ConnectionWriteTimeout: time.Second * 2,
	ReplicationConfig: &replication.ReplicationConfig{
		Role:               "master",
		MasterReplOffset:   atomic.Uint64{},
		SecondReplOffset:   atomic.Uint64{},
		ConnectedSlaves:    atomic.Uint64{},
		ReplBacklogActive:  0,
		ReplBacklogSize:    1048576,
		ReplBacklogFirst:   0,
		ReplBacklogHistlen: 0,
	},
	PersistenceConfig: &persistence.Config{
		Dir:  ".",
		File: "dump.rdb",
	},
}
