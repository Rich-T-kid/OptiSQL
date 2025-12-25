package substrait

import (
	"context"
	"fmt"
	"net"
	"opti-sql-go/Expr"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
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

func TestSubstraitSourceParse(t *testing.T) {
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
func TestExpressionsParse(t *testing.T) {
	// (1) all required fields exist
	// (2) fields contain valid set of values (important for scalar functions and binary expr)
	correctExpr := func(e Expr.Expression, wantedExpr string) bool {
		switch e.(type) {
		case *Expr.Alias:
			return wantedExpr == "Alias"
		case *Expr.ColumnResolve:
			return wantedExpr == "ColumnResolve"
		case *Expr.LiteralResolve:
			return wantedExpr == "LiteralResolve"
		case *Expr.BinaryExpr:
			return wantedExpr == "BinaryExpr"
		case *Expr.ScalarFunction:
			return wantedExpr == "ScalarFunction"
		case *Expr.CastExpr:
			return wantedExpr == "CastExpr"
		case *Expr.NullCheckExpr:
			return wantedExpr == "NullCheckExpr"
		default:
			return false
		}
	}
	t.Run("Column Resolve Test", func(t *testing.T) {
		test := []struct {
			testName       string
			jsonBody       jsonOBJ
			expectedColumn string
			wantedExpreStr string
			expectedError  bool
		}{
			{
				testName: "basic column resolve",
				jsonBody: map[string]any{
					"expr_type": "ColumnResolve",
					"name":      "a",
				},
				expectedColumn: "a",
				wantedExpreStr: "ColumnResolve",
				expectedError:  false,
			},
			{
				testName: "column resolve with extra fields (ignored)",
				jsonBody: map[string]any{
					"expr_type": "ColumnResolve",
					"name":      "user_id",
					"junk":      "should be ignored",
					"num":       123,
				},
				expectedColumn: "user_id",
				wantedExpreStr: "ColumnResolve",
				expectedError:  false,
			},
			{
				testName: "missing name field",
				jsonBody: map[string]any{
					"expr_type": "ColumnResolve",
				},
				expectedColumn: "",
				wantedExpreStr: "ColumnResolve",
				expectedError:  true,
			},
			{
				testName: "name is wrong type (number)",
				jsonBody: map[string]any{
					"expr_type": "ColumnResolve",
					"name":      123,
				},
				expectedColumn: "",
				wantedExpreStr: "ColumnResolve",
				expectedError:  true,
			},
			{
				testName: "name is empty string",
				jsonBody: map[string]any{
					"expr_type": "ColumnResolve",
					"name":      "",
				},
				expectedColumn: "",
				wantedExpreStr: "ColumnResolve",
				expectedError:  true,
			},
			{
				testName: "expr_type wrong / missing (should fail)",
				jsonBody: map[string]any{
					// "expr_type": "ColumnResolve",
					"name": "a",
				},
				expectedColumn: "",
				wantedExpreStr: "ColumnResolve",
				expectedError:  true,
			},
		}
		for _, tt := range test {
			t.Run(tt.testName, func(t *testing.T) {
				expr, err := parseExpression(tt.jsonBody)
				if tt.expectedError {
					if err == nil {
						t.Fatalf("%s did not fail when expected to do so", tt.testName)
					}
					// Expected to fail and it failed -> test passes for this case.
					return
				}
				if err != nil {
					t.Fatalf("%s failed with error %v\n", tt.testName, err)
				}
				if !correctExpr(expr, tt.wantedExpreStr) {
					t.Errorf("%s recieved the incorrect expression, expected %v but recieved %v\n", tt.testName, tt.wantedExpreStr, expr)
				}
				cr, _ := expr.(*Expr.ColumnResolve)
				if cr.Name != tt.expectedColumn {
					t.Errorf("%s has incorrect column resolve name, expected %s but recieved %v\n", tt.testName, tt.expectedColumn, cr.Name)
				}
			})
		}
		// one for each type of accepted expression
	})
	// ! test every literal type
	t.Run("Literal Resolve Test", func(t *testing.T) {
		const exprName = "LiteralResolve"
		test := []struct {
			testName       string
			jsonBody       jsonOBJ
			expectedValue  any
			expectedType   arrow.DataType
			wantedExpreStr string
			expectedError  bool
		}{
			{
				testName: "basic Literal Resolve",
				jsonBody: map[string]any{
					"expr_type": "LiteralResolve",
					"value":     10,
					"lit_type":  "int",
				},
				expectedValue:  int32(10),
				expectedType:   arrow.PrimitiveTypes.Int32,
				wantedExpreStr: exprName,
				expectedError:  false,
			},
			{
				testName: "string literal",
				jsonBody: map[string]any{
					"expr_type": "LiteralResolve",
					"value":     "hello",
					"lit_type":  "string",
				},
				expectedValue:  "hello",
				expectedType:   arrow.BinaryTypes.String,
				wantedExpreStr: exprName,
				expectedError:  false,
			},
			{
				testName: "boolean literal true",
				jsonBody: map[string]any{
					"expr_type": "LiteralResolve",
					"value":     true,
					"lit_type":  "boolean",
				},
				expectedValue:  true,
				expectedType:   arrow.FixedWidthTypes.Boolean,
				wantedExpreStr: exprName,
				expectedError:  false,
			},
			{
				testName: "float64 literal",
				jsonBody: map[string]any{
					"expr_type": "LiteralResolve",
					"value":     3.14159,
					"lit_type":  "float64",
				},
				expectedValue:  3.14159,
				expectedType:   arrow.PrimitiveTypes.Float64,
				wantedExpreStr: exprName,
				expectedError:  false,
			},
			{
				testName: "missing required field (lit_type)",
				jsonBody: map[string]any{
					"expr_type": "LiteralResolve",
					"value":     10,
				},
				expectedValue:  nil,
				expectedType:   nil,
				wantedExpreStr: exprName,
				expectedError:  true,
			},
			{
				testName: "invalid lit_type value",
				jsonBody: map[string]any{
					"expr_type": "LiteralResolve",
					"value":     10,
					"lit_type":  "int64", // not supported by your switch
				},
				expectedValue:  nil,
				expectedType:   nil,
				wantedExpreStr: exprName,
				expectedError:  true,
			},
		}
		for _, tt := range test {
			t.Run(tt.testName, func(t *testing.T) {
				expr, err := parseExpression(tt.jsonBody)
				if tt.expectedError {
					if err == nil {
						t.Fatalf("%s did not fail when expected to do so", tt.testName)

					}
					return
				}
				if err != nil {
					t.Fatalf("%s failed with unexpected error %v\n", tt.testName, err)
				}
				if !correctExpr(expr, "LiteralResolve") {
					t.Errorf("%s recieved the incorrect expression, expected %v but recieved %v\n", tt.testName, tt.wantedExpreStr, expr)

				}
				lr, _ := expr.(*Expr.LiteralResolve)
				if lr.Value != tt.expectedValue {
					t.Fatalf("%s received incorrect value: expected (%T) %v, got (%T) %v",
						tt.testName, tt.expectedValue, tt.expectedValue, lr.Value, lr.Value,
					)
				}
			})
		}

		// one for each type of accepted expression
	})
	// ! test every binary operator, use table test to reduce lines taken up
	t.Run("BinaryExpr Test", func(t *testing.T) {
		// one for each type of accepted expression
	})
	// ! test every scalr function
	t.Run("Scalar Function Test", func(t *testing.T) {
		const exprName = "ScalarFunction"

		test := []struct {
			testName      string
			jsonBody      jsonOBJ
			expectedFunc  string
			expectedError bool
		}{
			// ---- VALID ----
			{
				testName: "Upper is valid",
				jsonBody: map[string]any{
					"expr_type": "ScalarFunction",
					"func":      "Upper",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
				},
				expectedFunc:  "Upper",
				expectedError: false,
			},
			{
				testName: "Lower is valid",
				jsonBody: map[string]any{
					"expr_type": "ScalarFunction",
					"func":      "Lower",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
				},
				expectedFunc:  "Lower",
				expectedError: false,
			},
			{
				testName: "Abs is valid",
				jsonBody: map[string]any{
					"expr_type": "ScalarFunction",
					"func":      "Abs",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
				},
				expectedFunc:  "Abs",
				expectedError: false,
			},
			{
				testName: "Round is valid",
				jsonBody: map[string]any{
					"expr_type": "ScalarFunction",
					"func":      "Round",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
				},
				expectedFunc:  "Round",
				expectedError: false,
			},

			// ---- INVALID ----
			{
				testName: "invalid scalar function name",
				jsonBody: map[string]any{
					"expr_type": "ScalarFunction",
					"func":      "NotARealFunc",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
				},
				expectedFunc:  "",
				expectedError: true,
			},
			{
				testName: "missing func field",
				jsonBody: map[string]any{
					"expr_type": "ScalarFunction",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
				},
				expectedFunc:  "",
				expectedError: true,
			},
			{
				testName: "func wrong type",
				jsonBody: map[string]any{
					"expr_type": "ScalarFunction",
					"func":      123,
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
				},
				expectedFunc:  "",
				expectedError: true,
			},
			{
				testName: "missing expr field",
				jsonBody: map[string]any{
					"expr_type": "ScalarFunction",
					"func":      "Upper",
				},
				expectedFunc:  "",
				expectedError: true,
			},
		}

		for _, tt := range test {
			t.Run(tt.testName, func(t *testing.T) {
				expr, err := parseExpression(tt.jsonBody)

				if tt.expectedError {
					if err == nil {
						t.Fatalf("%s did not fail when expected to do so", tt.testName)
					}
					return
				}

				if err != nil {
					t.Fatalf("%s failed with unexpected error %v", tt.testName, err)
				}

				if !correctExpr(expr, exprName) {
					t.Fatalf("%s received incorrect expression, expected %s but received %T",
						tt.testName, exprName, expr,
					)
				}

				sf, ok := expr.(*Expr.ScalarFunction)
				if !ok {
					t.Fatalf("%s expected *Expr.ScalarFunction but received %T", tt.testName, expr)
				}

				// NOTE: if your struct field is named differently, change sf.Func below.
				if sf.Function != Expr.FnToScalarFunction(tt.expectedFunc) {
					t.Fatalf("%s received incorrect scalar function, expected %q but received %q",
						tt.testName, tt.expectedFunc, sf.Function,
					)
				}
			})
		}
	})

	t.Run("Alias Test", func(t *testing.T) {
		// one for each type of accepted expression
	})
	t.Run("CastExpr Test", func(t *testing.T) {
		// one for each type of accepted expression
	})
	t.Run("NullCheckExpr Test", func(t *testing.T) {
		// one for each type of accepted expression
	})
}
func TestSubstraitProjectParse(t *testing.T) {
	t.Run("basic project operations", func(t *testing.T) {
		projectTestID := "project parse test special ID"
		lpMetaData := NewPlanMetaData(projectTestID)
		tests := []struct {
			testName    string
			logicalPlan jsonOBJ
			expectError bool
		}{
			{testName: "project all coluns",
				logicalPlan: map[string]any{},
				expectError: false,
			},
			{testName: "project some columns",
				logicalPlan: map[string]any{},
				expectError: false,
			},
			{testName: "project zero columns (should fail)",
				logicalPlan: map[string]any{},
				expectError: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.testName, func(t *testing.T) {
				proj, err := parseProject(tt.logicalPlan, lpMetaData)
				if err != nil && !tt.expectError {
					t.Errorf("unexpected error %v", err)
				}
				basicBatch, _ := proj.Next(5)
				t.Logf("%v\n", basicBatch.PrettyPrint())
			})
		}

	})
	t.Run("parsing alias in project", func(t *testing.T) {
		projectAliasID := "project test special ID"
		lpMetaData := NewPlanMetaData(projectAliasID)
		tests := []struct {
			testName    string
			logicalPlan jsonOBJ
			expectError bool
		}{
			{
				testName:    "provide alias for all columns",
				logicalPlan: map[string]any{},
				expectError: false,
			},
			{
				testName:    "provide alias no columns",
				logicalPlan: map[string]any{},
				expectError: false,
			},
			{
				testName:    "provide alias for some columns",
				logicalPlan: map[string]any{},
				expectError: false,
			},
			{
				testName:    "project colummns and alias column count arent aligned",
				logicalPlan: map[string]any{},
				expectError: true,
			},
			{
				testName:    "project colummns and alias column count arent aligned",
				logicalPlan: map[string]any{},
				expectError: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.testName, func(t *testing.T) {
				proj, err := parseProject(tt.logicalPlan, lpMetaData)
				if err != nil && !tt.expectError {
					t.Errorf("unexpected error %v", err)
				}
				basicBatch, _ := proj.Next(5)
				t.Logf("%v\n", basicBatch.PrettyPrint())
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
