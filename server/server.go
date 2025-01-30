package server

import (
	"context"
	"fmt"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/mateopresacastro/mokv/api"
	"github.com/mateopresacastro/mokv/kv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	status "google.golang.org/grpc/status"
)

type kvServer struct {
	api.KVServer
	KV           kv.KV
	authorizer   Authorizer
	serverGetter kv.ServerProvider
}

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type Authorizer interface {
	Authorize(subject, object, action string) error
}

func NewServerGetter(kv kv.KV) kv.ServerProvider {
	return &kvServerGetter{kv: kv}
}

type kvServerGetter struct {
	kv kv.KV
}

func (kg *kvServerGetter) GetServers() ([]*api.Server, error) {
	if provider, ok := kg.kv.(kv.ServerProvider); ok {
		return provider.GetServers()
	}
	return nil, fmt.Errorf("kv store does not support getting servers")
}

func New(KV kv.KV, authorizer Authorizer, opts ...grpc.ServerOption) *grpc.Server {
	// Middleware for streaming and unary requests
	opts = append(opts, grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			grpc_auth.StreamServerInterceptor(authenticate),
		),
	), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
		grpc_auth.UnaryServerInterceptor(authenticate),
	)))

	s := grpc.NewServer(opts...)
	serverGetter := NewServerGetter(KV)
	srv := &kvServer{
		KV:           KV,
		serverGetter: serverGetter,
		authorizer:   authorizer,
	}
	api.RegisterKVServer(s, srv)
	return s
}

func (s *kvServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	err := s.authorizer.Authorize(subject(ctx), objectWildcard, consumeAction)
	if err != nil {
		return nil, err
	}
	value, err := s.KV.Get(req.Key)
	if err != nil {
		return nil, status.New(codes.NotFound, s.notFoundMsg(req.Key)).Err() // TODO improve error handling. Do boundary layers.
	}
	return &api.GetResponse{Value: value}, nil
}

func (s *kvServer) Set(ctx context.Context, req *api.SetRequest) (*api.SetResponse, error) {
	err := s.authorizer.Authorize(subject(ctx), objectWildcard, produceAction)
	if err != nil {
		return nil, err
	}
	err = s.KV.Set(req.Key, req.Value)
	if err != nil {
		return &api.SetResponse{Ok: false}, status.New(codes.Internal, "something went wrong storing data").Err()
	}
	return &api.SetResponse{Ok: true}, nil
}

func (s *kvServer) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	err := s.authorizer.Authorize(subject(ctx), objectWildcard, produceAction)
	if err != nil {
		return nil, err
	}
	err = s.KV.Delete(req.Key)
	if err != nil {
		return nil, status.New(codes.NotFound, s.notFoundMsg(req.Key)).Err()
	}
	return &api.DeleteResponse{Ok: true}, nil
}

func (s *kvServer) List(req *api.Empty, stream grpc.ServerStreamingServer[api.GetResponse]) error {
	if err := s.authorizer.Authorize(subject(stream.Context()), objectWildcard, consumeAction); err != nil {
		return err
	}
	for value := range s.KV.List() {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
			if err := stream.Send(&api.GetResponse{Value: value}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *kvServer) GetServers(ctx context.Context, req *api.Empty) (*api.GetServersResponse, error) {
	servers, err := s.serverGetter.GetServers()
	if err != nil {
		return nil, err
	}
	return &api.GetServersResponse{Servers: servers}, nil
}

func (s *kvServer) notFoundMsg(key string) string {
	return fmt.Sprintf("no value for key: %s", key)
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}

func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(codes.Unknown, "couldn't find peer info").Err()
	}
	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ctx, status.New(codes.Unauthenticated, "invalid auth info type").Err()
	}

	if len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return ctx, status.New(codes.Unauthenticated, "no valid certificate found").Err()
	}

	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)
	return ctx, nil
}
