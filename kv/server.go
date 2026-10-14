package kv

// Server is a node in the cluster.
type Server struct {
	ID string
	// Addr is the address the node serves clients and Raft on.
	Addr     string
	IsLeader bool
}
