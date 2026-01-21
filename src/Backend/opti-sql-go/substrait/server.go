package substrait

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"opti-sql-go/config"
	"opti-sql-go/operators/project"
	"os"
	"os/signal"
	"strings"
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
	decodedPlan, err := base64.StdEncoding.DecodeString(req.LogicalPlan)
	if err != nil {
		return nil, fmt.Errorf("failed to base64 decode logical plan: %w", err)
	}
	fmt.Printf("Received query request: logical_plan:%s\n sql:%v\n id:%v\n", decodedPlan, req.SqlStatement, req.Id)
	planM := newPlanMetaData(req.Id)
	source := strings.NewReader(string(decodedPlan))
	results, err := consumePlan(source, planM)
	if err != nil {
		return &QueryExecutionResponse{
			S3ResultLink: "NAN",
			ErrorType: &ErrorDetails{
				ErrorType: ReturnTypes_PARSE_ERROR,
				Message:   err.Error(),
			},
		}, nil
	}
	rc, err := results.consumeAll()
	if err != nil {
		return &QueryExecutionResponse{
			S3ResultLink: "NAN",
			ErrorType: &ErrorDetails{
				ErrorType: ReturnTypes_EXECUTION_ERROR,
				Message:   err.Error(),
			},
		}, nil

	}
	csv, err := rc.ToCSV()
	if err != nil {
		return &QueryExecutionResponse{
			S3ResultLink: "NAN",
			ErrorType: &ErrorDetails{
				ErrorType: ReturnTypes_UPLOAD_ERROR,
				Message:   err.Error(),
			},
		}, nil

	}
	// include random number for the sake of avoiding conflicts, should resolve this at the
	// logical processing step but for now this works
	fName := fmt.Sprintf("%s-%s-%d", strings.ReplaceAll(req.SqlStatement, " ", "-"), req.Id, rand.IntN(1000))
	if err = project.UploadResults(fName, csv); err != nil {
		return &QueryExecutionResponse{
			S3ResultLink: "NAN",
			ErrorType: &ErrorDetails{
				ErrorType: ReturnTypes_UPLOAD_ERROR,
				Message:   err.Error(),
			},
		}, nil

	}

	// Placeholder response
	return &QueryExecutionResponse{
		S3ResultLink: fName,
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
	fmt.Printf("Execution server is running on %s:%d", c.Server.Host, c.Server.Port)
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
	os.Exit(1)
}
