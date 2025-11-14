package substrait

import (
	"context"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
)

// SubstraitServer receives the substrait plan (gRPC) and sends out the optimized substrait plan (gRPC)
type SubstraitServer struct {
	UnimplementedSSOperationServer
}

func newSubstraitServer() *SubstraitServer {
	return &SubstraitServer{}
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

func Start() {
	port := 8000
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", port, err)
	}

	grpcServer := grpc.NewServer()
	RegisterSSOperationServer(grpcServer, newSubstraitServer())

	log.Printf("Substrait server listening on port %d", port)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
