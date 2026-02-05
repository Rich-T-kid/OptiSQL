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

	"go.uber.org/zap"
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
func (s *SubstraitServer) ExecuteQuery(ctx context.Context, req *QueryExecutionRequest) (resp *QueryExecutionResponse, err error) {
	logger := config.GetLogger()

	// Panic recovery to prevent one failing query from taking down the entire server
	defer func() {
		if r := recover(); r != nil {
			logger.Error("Panic recovered in ExecuteQuery",
				zap.Any("panic", r),
				zap.String("query_id", req.Id),
				zap.String("sql", req.SqlStatement),
				zap.Stack("stack_trace"),
			)
			resp = &QueryExecutionResponse{
				S3ResultLink: "NAN",
				ErrorType: &ErrorDetails{
					ErrorType: ReturnTypes_EXECUTION_ERROR,
					Message:   fmt.Sprintf("Internal server error: panic recovered: %v", r),
				},
			}
			err = nil // Return error as part of response, not as gRPC error
		}
	}()

	logger.Info("Received query execution request",
		zap.String("query_id", req.Id),
		zap.String("sql", req.SqlStatement),
		zap.Int("plan_size_bytes", len(req.LogicalPlan)),
	)

	decodedPlan, err := base64.StdEncoding.DecodeString(req.LogicalPlan)
	if err != nil {
		logger.Error("Failed to decode base64 plan",
			zap.Error(err),
			zap.String("query_id", req.Id),
		)
		return nil, fmt.Errorf("failed to base64 decode logical plan: %w", err)
	}
	logger.Debug("Plan decoded successfully", zap.Int("decoded_size_bytes", len(decodedPlan)))
	logger.Debug("Received query request details", zap.String("logical_plan", string(decodedPlan)), zap.String("sql", req.SqlStatement), zap.String("id", req.Id))
	planM := newPlanMetaData(req.Id)
	source := strings.NewReader(string(decodedPlan))

	logger.Info("Parsing logical plan", zap.String("query_id", req.Id))
	results, err := consumePlan(source, planM)
	if err != nil {
		logger.Error("Failed to parse logical plan",
			zap.Error(err),
			zap.String("query_id", req.Id),
		)
		return &QueryExecutionResponse{
			S3ResultLink: "NAN",
			ErrorType: &ErrorDetails{
				ErrorType: ReturnTypes_PARSE_ERROR,
				Message:   err.Error(),
			},
		}, nil
	}

	logger.Info("Executing query plan", zap.String("query_id", req.Id))
	rc, err := results.consumeAll()
	if err != nil {
		logger.Error("Failed to execute query plan",
			zap.Error(err),
			zap.String("query_id", req.Id),
		)
		return &QueryExecutionResponse{
			S3ResultLink: "NAN",
			ErrorType: &ErrorDetails{
				ErrorType: ReturnTypes_EXECUTION_ERROR,
				Message:   err.Error(),
			},
		}, nil

	}

	logger.Info("Converting results to CSV",
		zap.String("query_id", req.Id),
		zap.Uint64("row_count", rc.RowCount),
	)
	csv, err := rc.ToCSV()
	if err != nil {
		logger.Error("Failed to convert results to CSV",
			zap.Error(err),
			zap.String("query_id", req.Id),
		)
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
	// ! todo: finish debugging
	logger.Debug("CSV file produced", zap.String("file_name", fName), zap.String("csv_content", string(csv)))

	logger.Info("Uploading results to S3",
		zap.String("query_id", req.Id),
		zap.String("file_name", fName),
		zap.Int("csv_size_bytes", len(csv)),
	)
	if err = project.UploadResults(fName, csv); err != nil {
		logger.Error("Failed to upload results to S3",
			zap.Error(err),
			zap.String("query_id", req.Id),
			zap.String("file_name", fName),
		)
		return &QueryExecutionResponse{
			S3ResultLink: "NAN",
			ErrorType: &ErrorDetails{
				ErrorType: ReturnTypes_UPLOAD_ERROR,
				Message:   err.Error(),
			},
		}, nil

	}

	logger.Info("Query executed successfully",
		zap.String("query_id", req.Id),
		zap.String("s3_link", fName),
		zap.Uint64("result_rows", rc.RowCount),
	)

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
	logger := config.GetLogger()
	logger.Info("Execution server is running", zap.String("host", c.Server.Host), zap.Int("port", c.Server.Port))
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
	logger := config.GetLogger()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case <-stopChan:
		logger.Info("Shutdown requested by caller")
	case sig := <-sigChan:
		logger.Info("Received signal", zap.String("signal", sig.String()))
	}

	l := *s.listener
	_ = l.Close()

	grpcServer.GracefulStop()

	logger.Info("Server shutdown complete")
	os.Exit(1)
}
