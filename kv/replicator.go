package kv

import (
	"context"
	"log/slog"
	"sync"

	"github.com/mateopresacastro/mokv/api"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.KVClient
	mu          sync.Mutex
	servers     map[string]chan struct{}
	closed      bool
	close       chan struct{}
}

func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	if _, ok := r.servers[name]; ok {
		return nil
	}
	r.servers[name] = make(chan struct{})
	go r.replicate(addr, r.servers[name])
	return nil
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.NewClient(addr, r.DialOptions...)
	if err != nil {
		slog.Error("failed to dial", "err", err)
		return
	}
	defer cc.Close()

	client := api.NewKVClient(cc)
	ctx := context.Background()

	stream, err := client.List(ctx, &api.Empty{})
	if err != nil {
		slog.Error("failed to get", "err", err)
		return
	}
	responses := make(chan *api.GetResponse)

	go func() {
		defer close(responses)
		for {
			recv, err := stream.Recv()
			if err != nil {
				slog.Error("failed to receive", "err", err)
				return
			}

			responses <- recv
		}
	}()

	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case response := <-responses:
			_, err = r.LocalServer.Set(ctx, &api.SetRequest{Key: response.Key, Value: response.Value})
			if err != nil {
				slog.Error("failed to set", "err", err)
				return
			}
		}
	}
}

func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

func (r *Replicator) init() {
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}
