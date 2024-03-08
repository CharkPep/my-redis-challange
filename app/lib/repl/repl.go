package repl

type Replica struct {
	host string
	port int
}

func NewReplica(host string, port int) *Replica {
	return &Replica{
		host: host,
		port: port,
	}
}
