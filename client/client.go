// Package client is a Go client for a mokv cluster. It also registers a gRPC
// resolver and balancer for the Scheme, which send writes to the leader and
// spread reads across followers.
package client

import (
	"context"
	"time"

	"github.com/dynamic-calm/mokv/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client provides access to a mökv cluster.
type Client struct {
	conn *grpc.ClientConn
	api  api.KVClient
}

// Server represents a node in the cluster.
type Server struct {
	RPCAddr  string
	IsLeader bool
	ID       string
}

// New connects to a mökv node at the given address.
func New(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, api: api.NewKVClient(conn)}, nil
}

// Get retrieves a value by key. If the key does not exist, the error has
// code NotFound.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	res, err := c.api.Get(ctx, &api.GetRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return res.Value, nil
}

// Set stores value under key for ttl, replicating it across the cluster.
func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	_, err := c.api.Set(ctx, &api.SetRequest{Key: key, Value: value, Ttl: durationpb.New(ttl)})
	return err
}

// Delete removes key from the cluster.
func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.api.Delete(ctx, &api.DeleteRequest{Key: key})
	return err
}

// Servers returns all nodes in the cluster.
func (c *Client) Servers(ctx context.Context) ([]Server, error) {
	res, err := c.api.GetServers(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	servers := make([]Server, len(res.Servers))
	for i, s := range res.Servers {
		servers[i] = Server{RPCAddr: s.RpcAddr, IsLeader: s.IsLeader, ID: s.Id}
	}
	return servers, nil
}

// Close releases the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
