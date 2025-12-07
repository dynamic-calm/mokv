package mokv

import (
	"context"

	"github.com/dynamic-calm/mokv/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client provides access to a mökv cluster.
type Client struct {
	conn *grpc.ClientConn
	api  api.KVClient
}

// Server represents a node in the cluster.
type Server struct {
	RpcAddr  string
	IsLeader bool
	ID       string
}

// NewClient connects to a mökv node at the given address.
func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		api:  api.NewKVClient(conn),
	}, nil
}

// Get retrieves a value by key. Returns nil if the key does not exist.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	res, err := c.api.Get(ctx, &api.GetRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return res.Value, nil
}

// Set stores a key-value pair, replicating it across the cluster.
func (c *Client) Set(ctx context.Context, key string, value []byte) (bool, error) {
	res, err := c.api.Set(ctx, &api.SetRequest{Key: key, Value: value})
	return res.Ok, err
}

// Delete removes a key from the cluster.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	res, err := c.api.Delete(ctx, &api.DeleteRequest{Key: key})
	return res.Ok, err
}

// GetServers returns all nodes in the cluster.
func (c *Client) GetServers(ctx context.Context) ([]Server, error) {
	res, err := c.api.GetServers(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	servers := make([]Server, len(res.Servers))
	for i, s := range res.Servers {
		servers[i] = Server{RpcAddr: s.RpcAddr, IsLeader: s.IsLeader, ID: s.Id}
	}
	return servers, nil
}

// Close releases the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
