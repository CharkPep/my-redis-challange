package repl

import (
	"log"
	"sync"
)

type ReplicaManager struct {
	replicas []*Replica
	mu       sync.Mutex
}

func (m *ReplicaManager) AddReplica(r *Replica) {
	m.mu.Lock()
	m.replicas = append(m.replicas, r)
	m.mu.Unlock()
}

func (m *ReplicaManager) PropagateToAll(buffer []byte) error {
	for _, r := range m.replicas {
		log.Printf("Propagating %q to %q", buffer, r.conn.RemoteAddr())
		// Note: should do propagation in separate go routine
		// TODO: handle errors, if replica is not reachable, remove from pool, etc
		if _, err := r.conn.Write(buffer); err != nil {
			log.Printf("Error propagating to %s", r.conn.RemoteAddr())
			return err
		}
	}

	return nil
}

func NewReplicaManager() *ReplicaManager {
	return &ReplicaManager{
		replicas: make([]*Replica, 0, 4),
		mu:       sync.Mutex{},
	}
}
