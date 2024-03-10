package repl

func (m *ReplicaManager) AddReplica(r *Replica) {
	m.replicas = append(m.replicas, *r)
}

func (m *ReplicaManager) PropagateToAll(buffer []byte) error {
	for _, r := range m.replicas {
		// Note: should do propagation in separate go routine
		// TODO: handle errors, if replica is not reachable, remove from pool, etc
		r.Send(buffer)
	}

	return nil
}

func NewReplicaManager() *ReplicaManager {
	return &ReplicaManager{
		replicas: make([]Replica, 0, 4),
	}
}
