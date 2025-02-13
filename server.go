package mokv

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/mateopresacastro/mokv/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type kvServer struct {
	api.KVServer
	KV           KVI
	authorizer   Authorizer
	serverGetter ServerProvider
}

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type Authorizer interface {
	Authorize(subject, object, action string) error
}

func NewServerGetter(kv KVI) ServerProvider {
	return &kvServerGetter{kv: kv}
}

type kvServerGetter struct {
	kv KVI
}

func (kg *kvServerGetter) GetServers() ([]*api.Server, error) {
	if provider, ok := kg.kv.(ServerProvider); ok {
		return provider.GetServers()
	}
	return nil, fmt.Errorf("kv store does not support getting servers")
}

func NewServer(KV KVI, authorizer Authorizer, opts ...grpc.ServerOption) *grpc.Server {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	logOpts := []logging.Option{
		logging.WithLogOnEvents(logging.StartCall, logging.FinishCall),
	}

	// Middleware for streaming and unary requests
	opts = append(opts, grpc.StreamInterceptor(
		grpc_middleware.ChainStreamServer(
			logging.StreamServerInterceptor(interceptorLogger(logger), logOpts...),
			grpc_auth.StreamServerInterceptor(authenticate),
		),
	), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
		logging.UnaryServerInterceptor(interceptorLogger(logger), logOpts...),
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
	return &api.GetResponse{Value: value, Key: req.Key}, nil
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

func (s *kvServer) List(req *emptypb.Empty, stream grpc.ServerStreamingServer[api.GetResponse]) error {
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

func (s *kvServer) GetServers(ctx context.Context, req *emptypb.Empty) (*api.GetServersResponse, error) {
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

func loggerInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()

	// Extract peer information
	peer, _ := peer.FromContext(ctx)
	peerAddr := "unknown"
	if peer != nil {
		peerAddr = peer.Addr.String()
	}

	// Extract metadata
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	}

	// Extract subject from context
	sub, _ := ctx.Value(subjectContextKey{}).(string)

	// Create logger with request context
	reqLogger := slog.With(
		"method", info.FullMethod,
		"peer_address", peerAddr,
		"subject", sub,
		"user_agent", firstOrEmpty(md.Get("user-agent")),
		"request_id", firstOrEmpty(md.Get("x-request-id")),
	)

	reqLogger.Info("handling request")

	// Call the handler
	resp, err := handler(ctx, req)

	// Calculate duration
	duration := time.Since(start)

	// Log the result
	if err != nil {
		st, _ := status.FromError(err)
		reqLogger.Error("request failed",
			"code", st.Code(),
			"error", err.Error(),
			"duration_ms", duration.Milliseconds(),
		)
	} else {
		reqLogger.Info("request completed",
			"duration_ms", duration.Milliseconds(),
		)
	}

	return resp, err
}

func firstOrEmpty(values []string) string {
	if len(values) > 0 {
		return values[0]
	}
	return ""
}

func interceptorLogger(l *slog.Logger) logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		l.Log(ctx, slog.Level(lvl), msg, fields...)
	})
}
