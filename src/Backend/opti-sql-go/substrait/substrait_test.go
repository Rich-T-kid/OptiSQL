package substrait

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
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
		SqlStatement: "SELECT * FROM table",
		LogicalPlan:  "CgJTUxIMCgpTZWxlY3QgKiBGUk9NIHRhYmxl",
		Id:           "GenerateDTMoneyOHaasdavdasvasdvada",
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

// Plan parsing
const customIRPath = "../../test_data/substrait_plans/basic"

func TestSubstraitPlanExist(t *testing.T) {
	e, err := os.ReadDir(customIRPath)
	if err != nil {
		t.Fatalf("failed to open dir with error: %v\n", e)
	}
	for entries, name := range e {
		fmt.Printf("entrie[%v]:\t%v\n", entries, name)
	}

}

func TestSubstraitEmitParse(t *testing.T) {
	t.Run("b0_00_test", func(t *testing.T) {
		/*fileName := "basic_00_test.json"
		sourceFile := fmt.Sprintf("%v/%v", customIRPath, fileName)
		f, err := os.Open(sourceFile)
		if err != nil {
			t.Fatalf("failed to open %s, error returned:\t%v", fileName, err)
		}
		e, err := consumePlan(f)
		if err != nil {
			t.Fatalf("error occured reading plan: %v", err)
		}
		t.Logf("recieved final emmiter :%v\n", e)*/
		name := "temp.23.12.csv"
		pieces := strings.Split(name, ".")
		fmt.Printf("pieces:\t%v\n", pieces)
		lastPiece := pieces[len(pieces)-1]
		fmt.Printf("last pieces:\t%v\n", lastPiece)

	})
	t.Run("basic_01_source_filter parse", func(t *testing.T) {
		fileName := "b1_01_source_filter.json"
		sourceFile := fmt.Sprintf("%v/%v", customIRPath, fileName)
		f, err := os.Open(sourceFile)
		if err != nil {
			t.Fatalf("failed to open %s, error returned:\t%v", fileName, err)
		}
		e, err := consumePlan(f, NewPlanMetaData("tmp"))
		if err != nil {
			t.Fatalf("error occured reading plan: %v", err)
		}
		t.Logf("recieved final emmiter :%v\n", e)

	})
}

func TestSubstraitUnit(t *testing.T) {
	t.Run("source parse test", func(t *testing.T) {
		tests := []struct {
			name      string
			fileName  string
			local     bool
			wantError bool
		}{
			{
				name:      "csv file",
				fileName:  "country_full.csv",
				local:     true,
				wantError: false,
			},
			{
				name:      "parquet file with local true",
				fileName:  "userdata.parquet",
				local:     true,
				wantError: false,
			},
			{
				name:      "parquet file with local false",
				fileName:  "userdata.parquet",
				local:     false,
				wantError: false,
			},
		}
		curDir, err := os.Getwd()
		if err != nil {
			t.Fatalf("failed to observe current working directory")
		}
		id := "richards-test-substrait-22"

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				sourceObj := jsonOBJ{
					"file-name": tt.fileName,
					"local":     tt.local,
				}

				op, err := parseSource(sourceObj, NewPlanMetaData(id))
				if (err != nil) != tt.wantError {
					t.Errorf("parseSource() error = %v, wantError %v", err, tt.wantError)
					return
				}
				if !tt.wantError && op == nil {
					t.Errorf("parseSource() returned nil operator when error was nil")
				}
				if tt.local {
					path := fmt.Sprintf("%s/%s-%s", curDir, tt.fileName, id)
					fmt.Printf("attempting to remove %s from path\n", path)
					if err := os.Remove(path); err != nil {
						t.Errorf("test:%s\n failed to delete %s from file system \n", tt.name, tt.fileName)
					}

				}
			})
		}
	})
}

func TestContainsFields(t *testing.T) {
	tests := []struct {
		name      string
		fields    []string
		obj       jsonOBJ
		wantError bool
	}{
		{
			name:      "all fields present",
			fields:    []string{"file-name", "local"},
			obj:       jsonOBJ{"file-name": "test.csv", "local": true},
			wantError: false,
		},
		{
			name:      "missing single field",
			fields:    []string{"file-name", "local"},
			obj:       jsonOBJ{"file-name": "test.csv"},
			wantError: true,
		},
		{
			name:      "missing multiple fields",
			fields:    []string{"file-name", "local", "format"},
			obj:       jsonOBJ{"file-name": "test.csv"},
			wantError: true,
		},
		{
			name:      "extra fields present",
			fields:    []string{"file-name"},
			obj:       jsonOBJ{"file-name": "test.csv", "local": true, "extra": "field"},
			wantError: false,
		},
	}

	for _, tt := range tests {
		tt := tt // rebind for subtest safety

		t.Run(tt.name, func(t *testing.T) {
			err := containsFields(tt.fields, tt.obj)

			if tt.wantError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), "missing required fields") {
					t.Fatalf("unexpected error message: %q", err.Error())
				}
				return
			}

			// want no error
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
		})
	}
}
func TestCorrectFieldTypes(t *testing.T) {
	tests := []struct {
		name       string
		fields     []string
		fieldTypes []string
		obj        jsonOBJ
		wantError  bool
	}{
		{
			name:       "all types correct",
			fields:     []string{"file-name", "local"},
			fieldTypes: []string{"string", "boolean"},
			obj:        jsonOBJ{"file-name": "test.csv", "local": true},
			wantError:  false,
		},
		{
			name:       "single string type mismatch",
			fields:     []string{"file-name"},
			fieldTypes: []string{"string"},
			obj:        jsonOBJ{"file-name": 123},
			wantError:  true,
		},
		{
			name:       "single boolean type mismatch",
			fields:     []string{"local"},
			fieldTypes: []string{"boolean"},
			obj:        jsonOBJ{"local": "true"},
			wantError:  true,
		},
		{
			name:       "number type correct",
			fields:     []string{"count"},
			fieldTypes: []string{"number"},
			obj:        jsonOBJ{"count": float64(10)},
			wantError:  false,
		},
		{
			name:       "object type correct",
			fields:     []string{"meta"},
			fieldTypes: []string{"object"},
			obj:        jsonOBJ{"meta": jsonOBJ{"a": 1}},
			wantError:  false,
		},
		{
			name:       "array type correct",
			fields:     []string{"items"},
			fieldTypes: []string{"array"},
			obj:        jsonOBJ{"items": []any{1, 2, 3}},
			wantError:  false,
		},
		{
			name:       "mixed correct and incorrect types",
			fields:     []string{"file-name", "local"},
			fieldTypes: []string{"string", "boolean"},
			obj:        jsonOBJ{"file-name": "ok.csv", "local": "yes"},
			wantError:  true,
		},
		{
			name:       "multiple mismatches",
			fields:     []string{"file-name", "local"},
			fieldTypes: []string{"string", "boolean"},
			obj:        jsonOBJ{"file-name": 10, "local": "false"},
			wantError:  true,
		},
		{
			name:       "extra fields ignored",
			fields:     []string{"file-name"},
			fieldTypes: []string{"string"},
			obj:        jsonOBJ{"file-name": "test.csv", "extra": true},
			wantError:  false,
		},
		{
			name:       "field and type count mismatch",
			fields:     []string{"file-name", "local"},
			fieldTypes: []string{"string"},
			obj:        jsonOBJ{"file-name": "test.csv", "local": true},
			wantError:  true,
		},
		{
			name:       "empty fields and types",
			fields:     []string{},
			fieldTypes: []string{},
			obj:        jsonOBJ{},
			wantError:  false,
		},
	}

	for _, tt := range tests {
		tt := tt // rebind for subtest safety

		t.Run(tt.name, func(t *testing.T) {
			err := correctFieldTypes(tt.fields, tt.fieldTypes, tt.obj)

			if tt.wantError && err == nil {
				t.Fatalf("expected error, got nil")
			}

			if !tt.wantError && err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
		})
	}
}
