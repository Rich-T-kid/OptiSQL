package substrait

import (
	"context"
	"fmt"
	"log"
	"net"
	"opti-sql-go/config"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
)

// SubstraitServer receives the substrait plan (gRPC) and sends out the optimized substrait plan (gRPC)
type SubstraitServer struct {
	UnimplementedSSOperationServer
	listener *net.Listener
}

func newSubstraitServer(l *net.Listener) *SubstraitServer {
	return &SubstraitServer{
		listener: l,
	}
}

// ExecuteQuery implements the gRPC service method
func (s *SubstraitServer) ExecuteQuery(ctx context.Context, req *QueryExecutionRequest) (*QueryExecutionResponse, error) {
	fmt.Printf("Received query request: logical_plan:%v\n sql:%s\n id:%v\n source: %v\n", req.SubstraitLogical, req.SqlStatement, req.Id, req.Source)

	// Placeholder response
	return &QueryExecutionResponse{
		S3ResultLink: "",
		ErrorType: &ErrorDetails{
			ErrorType: ReturnTypes_SUCCESS,
			Message:   "Query executed successfully",
		},
	}, nil
}

func Start() chan struct{} {
	c := config.GetConfig()
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", c.Server.Port, err)
	}

	grpcServer := grpc.NewServer()
	ss := newSubstraitServer(&listener)
	RegisterSSOperationServer(grpcServer, ss)

	stopChan := make(chan struct{})

	log.Printf("Substrait server listening on port %d", c.Server.Port)
	go unifiedShutdownHandler(ss, grpcServer, stopChan)
	go func() {
		if err := grpcServer.Serve(*ss.listener); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()
	return stopChan
}
func unifiedShutdownHandler(s *SubstraitServer, grpcServer *grpc.Server, stopChan chan struct{}) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case <-stopChan:
		fmt.Println("Shutdown requested by caller.")
	case sig := <-sigChan:
		fmt.Printf("Received signal: %v\n", sig)
	}

	l := *s.listener
	_ = l.Close()

	grpcServer.GracefulStop()

	fmt.Println("Server shutdown complete")
}
