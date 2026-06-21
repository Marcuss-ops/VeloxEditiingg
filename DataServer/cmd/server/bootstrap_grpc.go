package main

import (
	"net"

	"velox-server/internal/config"
	"velox-server/internal/grpcserver"

	"google.golang.org/grpc"
)

type grpcServer interface {
	GracefulStop()
	Stop()
}

type grpcServerWrapper struct {
	Server   *grpc.Server
	Listener net.Listener
}

func (w *grpcServerWrapper) GracefulStop() { w.Server.GracefulStop() }
func (w *grpcServerWrapper) Stop()         { w.Server.Stop() }

func parseInsecureDevFlag(envVal string) bool {
	return envVal == "true"
}

func buildGRPCHandlerConfig(cfg *config.Config, insecureDev bool) *grpcserver.HandlerConfig {
	return &grpcserver.HandlerConfig{
		PushMode:       cfg.Server.GRPCPushMode,
		AllowInsecure:  insecureDev,
		AllowedWorkers: cfg.Workers.AllowedWorkers,
	}
}
