package lib

import (
	"context"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/lib/replication"
	"log"
)

func (s *RedisServer) PropagateToAll(buff []byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.logger.Printf("Propagating to all slaves, %d", len(s.slaves))
	for _, r := range s.slaves {
		if _, err := r.Propagate(buff); err != nil {
			s.logger.Printf("Error writing to replica: %s", err)
		}
	}
}

func (s *RedisServer) ConnectMaster() error {
	if s.config.ReplicaOf != "" {
		s.config.ReplicationConfig.Role = "slave"
		master, err := replication.NewReplicaOf(s.db, s.config.ReplicaOf, fmt.Sprint(s.config.Port), s.propagation)
		if err != nil {
			s.logger.Printf("Failed to connect to master %v: %s", s.config.ReplicaOf, err)
			return err
		}
		s.replicaOf = master
		go s.initPropagationConsumptionFromMaster()
	}
	return nil
}

func (s *RedisServer) initPropagationConsumptionFromMaster() {
	for i := 0; i < PROPAGATION_CONSUMERS; i++ {
		go func(i int) {
			for {
				select {
				case _, ok := <-s.close:
					if !ok {
						s.logger.Printf("Closing consumer %d", i)
						return
					}
				case req := <-s.propagation:
					handler, err := s.router.ResolveRequest(req.Args)
					if err != nil {
						s.logger.Printf("error resolving request: %s", err)
						return
					}

					rq := &RESPRequest{
						Args:        req.Args,
						Logger:      s.logger,
						s:           s,
						W:           req.Writer,
						Propagation: true,
					}

					if err := rq.SetDb(0); err != nil {
						s.logger.Printf("unexpected error setting db with idex %d", 0)
						return
					}

					req.Args.A = req.Args.A[1:]
					_, err = handler.HandleResp(context.Background(), rq)

					if err != nil {
						s.logger.Printf("error propagating to replica: %s", err)
						return
					}

					s.config.ReplicationConfig.MasterReplOffset.Add(uint64(req.N))
					log.Printf("Offset: %d", s.config.ReplicationConfig.MasterReplOffset.Load())
				}
			}
		}(i)
	}
}
