package substrait

import (
	"context"
	"net"
	"testing"
)

func TestInitServer(t *testing.T) {
	// Simple passing test
	l, err := net.Listen("tcp", "0.0.0.0:1212")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	ss := newSubstraitServer(&l)
	if ss == nil {
		t.Errorf("Expected non-nil Substrait server")
	}
}
func TestDummyInput(t *testing.T) {
	l, err := net.Listen("tcp", "0.0.0.0:1213")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	ss := newSubstraitServer(&l)
	if ss == nil {
		t.Errorf("Expected non-nil Substrait server")
	}
	dummyRequest := &QueryExecutionRequest{
		SqlStatement:     "SELECT * FROM table",
		SubstraitLogical: []byte("CgJTUxIMCgpTZWxlY3QgKiBGUk9NIHRhYmxl"),
		Id:               "GenerateDTMoneyOHaasdavdasvasdvada",
		Source: &SourceType{
			S3Source: "s3://my-bucket/data/table.parquet",
			Mime:     "application/vnd.apache.parquet",
		},
	}
	resp, err := ss.ExecuteQuery(context.Background(), dummyRequest)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if resp.ErrorType.ErrorType != ReturnTypes_SUCCESS {
		t.Errorf("Expected SUCCESS, got %v", resp.ErrorType.ErrorType)
	}
}

func TestStartServer(t *testing.T) {
	stopChan := Start()
	if stopChan == nil {
		t.Errorf("Expected non-nil stop channel")
	}

}
