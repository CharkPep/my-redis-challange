package e2e

import (
	"bufio"
	resp "github.com/codecrafters-io/redis-starter-go/app/lib/encoding"
	"net"
	"sync"
	"testing"
)

func TestHandshakeWithMaster(t *testing.T) {
	SetupMasterWithReplicationHandlers(t, MASTER_PORT)
	replica, err := net.Dial("tcp", ":6379")
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	rdb := resp.NewRdb(&sync.Map{})
	r := bufio.NewReader(replica)
	if err := EstablishReplicaMaster(rdb, replica, r); err != nil {
		t.Error(err)
	}
}
