package grpcserver

import (
	"context"
	"log/slog"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type Server struct {
	grpc   *grpc.Server
	health *health.Server
	logger *slog.Logger
}

func New(logger *slog.Logger) *Server {
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("pulsequeue.v1.JobService", grpc_health_v1.HealthCheckResponse_SERVING)

	grpcServer := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	return &Server{grpc: grpcServer, health: healthServer, logger: logger}
}

func (s *Server) Serve(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		s.grpc.GracefulStop()
	}()
	s.logger.Info("grpc server listening", "addr", addr)
	return s.grpc.Serve(listener)
}
