package substrait

import (
	"context"
	"fmt"
	"math"
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
		t.Logf("entrie[%v]:\t%v\n", entries, name)
	}

}

/*
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
*/

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
					t.Logf("attempting to remove %s from path\n", path)
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
				expectedValue:  int64(10),
				expectedType:   arrow.PrimitiveTypes.Int64,
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
	t.Run("BinaryExpr Test", func(t *testing.T) {
		const exprName = "ScalarFunction"

		validVariants := []string{
			"Addition",
			"Subtraction",
			"Multiplication",
			"Division",
			"Equal",
			"NotEqual",
			"LessThan",
			"LessThanOrEqual",
			"GreaterThan",
			"GreaterThanOrEqual",
			"And",
			"Or",
			"Like",
		}

		// Helper to keep JSON bodies consistent and small.
		mkBinary := func(op string) jsonOBJ {
			return map[string]any{
				"expr_type": "BinaryExpr",
				"op":        op,
				"left": map[string]any{
					"expr_type": "ColumnResolve",
					"name":      "a",
				},
				"right": map[string]any{
					"expr_type": "ColumnResolve",
					"name":      "b",
				},
			}
		}

		test := []struct {
			testName      string
			jsonBody      jsonOBJ
			operator      string
			expectedError bool
		}{}

		// --- Generate one passing test per valid operator variant ---
		for _, op := range validVariants {
			test = append(test, struct {
				testName      string
				jsonBody      jsonOBJ
				operator      string
				expectedError bool
			}{
				testName:      "operator propagates: " + op,
				jsonBody:      mkBinary(op),
				operator:      op,
				expectedError: false,
			})
		}
		test = append(test, struct {
			testName      string
			jsonBody      jsonOBJ
			operator      string
			expectedError bool
		}{
			testName:      "Empty Operator",
			jsonBody:      mkBinary(""),
			operator:      "",
			expectedError: true,
		},
		)
		test = append(test, struct {
			testName      string
			jsonBody      jsonOBJ
			operator      string
			expectedError bool
		}{
			testName:      "non-existant Operator",
			jsonBody:      mkBinary("matrixMultiply"),
			operator:      "matrixMultiply",
			expectedError: true,
		},
		)

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
				if !correctExpr(expr, "BinaryExpr") {
					t.Errorf("%s recieved the incorrect expression, expected %v but recieved %v of type %T\n", tt.testName, exprName, expr, expr)

				}
				BinaryExpr, _ := expr.(*Expr.BinaryExpr)
				if !Expr.MatchesBinaryOperator(tt.operator, int(BinaryExpr.Op)) {
					t.Errorf("%s mismatch between expected operator (%s) and the recieved operator (%v)", tt.testName, tt.operator, BinaryExpr.Op)
				}

			})
		}
		// one for each type of accepted expression
	})

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
		const exprName = "Alias"
		tests := []struct {
			testName    string
			jsonBody    jsonOBJ
			aliasName   string
			expectError bool
		}{
			{testName: "basic alias",
				jsonBody: map[string]any{
					"expr_type": "Alias",
					"name":      "new_name",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "first_column",
					},
				},
				aliasName:   "new_name",
				expectError: false,
			},
			{
				testName: "alias with different name",
				jsonBody: map[string]any{
					"expr_type": "Alias",
					"name":      "alias_1",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "col_a",
					},
				},
				aliasName:   "alias_1",
				expectError: false,
			},
			{
				testName: "missing alias name field",
				jsonBody: map[string]any{
					"expr_type": "Alias",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "first_column",
					},
				},
				aliasName:   "",
				expectError: true,
			},
			{
				testName: "alias name wrong type",
				jsonBody: map[string]any{
					"expr_type": "Alias",
					"name":      123,
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "first_column",
					},
				},
				aliasName:   "",
				expectError: true,
			},
			{
				testName: "missing expr field",
				jsonBody: map[string]any{
					"expr_type": "Alias",
					"name":      "new_name",
				},
				aliasName:   "",
				expectError: true,
			},
		}
		for _, tt := range tests {
			t.Run(tt.testName, func(t *testing.T) {
				expr, err := parseExpression(tt.jsonBody)
				if tt.expectError {
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

				alias, ok := expr.(*Expr.Alias)
				if !ok {
					t.Fatalf("%s expected *Expr.Alias but received %T", tt.testName, expr)
				}
				if alias.Name != tt.aliasName {
					t.Fatalf("%s recieved incorrect alias name, expected %s but recieved %s\n", tt.testName, tt.aliasName, alias.Name)
				}

			})
		}

	})
	t.Run("CastExpr Test", func(t *testing.T) {
		const exprName = "CastExpr"

		tests := []struct {
			testName       string
			jsonBody       jsonOBJ
			expectedToType string
			expectedError  bool
		}{
			// ---- VALID to_type ----
			{
				testName: "cast to int is valid",
				jsonBody: map[string]any{
					"expr_type": "CastExpr",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "a",
					},
					"to_type": "int",
				},
				expectedToType: "int",
				expectedError:  false,
			},
			{
				testName: "cast to string is valid",
				jsonBody: map[string]any{
					"expr_type": "CastExpr",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "a",
					},
					"to_type": "string",
				},
				expectedToType: "string",
				expectedError:  false,
			},
			{
				testName: "cast to boolean is valid",
				jsonBody: map[string]any{
					"expr_type": "CastExpr",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "a",
					},
					"to_type": "boolean",
				},
				expectedToType: "boolean",
				expectedError:  false,
			},
			{
				testName: "cast to float64 is valid",
				jsonBody: map[string]any{
					"expr_type": "CastExpr",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "a",
					},
					"to_type": "float64",
				},
				expectedToType: "float64",
				expectedError:  false,
			},

			// ---- INVALID to_type / malformed ----
			{
				testName: "invalid to_type value",
				jsonBody: map[string]any{
					"expr_type": "CastExpr",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "a",
					},
					"to_type": "int64",
				},
				expectedToType: "",
				expectedError:  true,
			},
			{
				testName: "missing to_type field",
				jsonBody: map[string]any{
					"expr_type": "CastExpr",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "a",
					},
				},
				expectedToType: "",
				expectedError:  true,
			},
			{
				testName: "to_type wrong type",
				jsonBody: map[string]any{
					"expr_type": "CastExpr",
					"expr": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "a",
					},
					"to_type": 123,
				},
				expectedToType: "",
				expectedError:  true,
			},
			{
				testName: "missing expr field",
				jsonBody: map[string]any{
					"expr_type": "CastExpr",
					"to_type":   "float64",
				},
				expectedToType: "",
				expectedError:  true,
			},
		}

		for _, tt := range tests {
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

				_, ok := expr.(*Expr.CastExpr)
				if !ok {
					t.Fatalf("%s expected *Expr.CastExpr but received %T", tt.testName, expr)
				}
			})
		}
	})
}

func TestSubstraitProjectParse(t *testing.T) {
	source1 := map[string]any{
		"Operator": "Source",
		"Source": map[string]any{
			"file-name": "country_full.csv",
			"local":     false,
		},
	}
	source2 := map[string]any{
		"Operator": "Source",
		"Source": map[string]any{
			"file-name": "fortune1000_2024.csv",
			"local":     false,
		},
	}

	t.Run("basic project operations", func(t *testing.T) {
		projectTestID := "project parse test special ID"
		lpMetaData := NewPlanMetaData(projectTestID)

		tests := []struct {
			testName    string
			logicalPlan jsonOBJ
			expectError bool
		}{
			{
				testName: "project all coluns",
				logicalPlan: map[string]any{
					"input": source1,
					"expressions": []map[string]any{
						{
							"expr_type": "ColumnResolve",
							"name":      "name",
						},
						{
							"expr_type": "ColumnResolve",
							"name":      "country-code",
						},
					},
				},
				expectError: false,
			},
			{
				testName: "project some columns",
				logicalPlan: map[string]any{
					"input": source2,
					"expressions": []map[string]any{
						{
							"expr_type": "ColumnResolve",
							"name":      "Company",
						},
					},
				},
				expectError: false,
			},
			{
				testName: "project zero columns (should fail)",
				logicalPlan: map[string]any{
					"input":       source1,
					"expressions": []map[string]any{},
				},
				expectError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.testName, func(t *testing.T) {
				_, err := parseProject(tt.logicalPlan, lpMetaData)

				if tt.expectError {
					if err == nil {
						t.Fatalf("%s did not fail when expected to do so", tt.testName)
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
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
				testName: "provide alias for all columns",
				logicalPlan: map[string]any{
					"input": source1,
					"expressions": []map[string]any{
						{
							"expr_type": "Alias",
							"name":      "country_name",
							"expr": map[string]any{
								"expr_type": "ColumnResolve",
								"name":      "name",
							},
						},
						{
							"expr_type": "Alias",
							"name":      "cc",
							"expr": map[string]any{
								"expr_type": "ColumnResolve",
								"name":      "country-code",
							},
						},
					},
				},
				expectError: false,
			},
			{
				testName: "provide alias no columns",
				logicalPlan: map[string]any{
					"input":       source2,
					"expressions": []map[string]any{},
				},
				expectError: true,
			},
			{
				testName: "provide alias for some columns",
				logicalPlan: map[string]any{
					"input": source2,
					"expressions": []map[string]any{
						{
							"expr_type": "Alias",
							"name":      "company_name",
							"expr": map[string]any{
								"expr_type": "ColumnResolve",
								"name":      "Company",
							},
						},
						{
							// no alias on this one (mix alias + plain column)
							"expr_type": "ColumnResolve",
							"name":      "Country",
						},
					},
				},
				expectError: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.testName, func(t *testing.T) {
				proj, err := parseProject(tt.logicalPlan, lpMetaData)

				if tt.expectError {
					if err == nil {
						t.Fatalf("%s did not fail when expected to do so", tt.testName)
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}

				// optional: sanity check the operator runs
				basicBatch, err := proj.Next(5)
				if err != nil {
					t.Fatalf("unexpected Next() error %v", err)
				}
				t.Logf("%v\n", basicBatch.PrettyPrint())
			})
		}
	})
}

func TestFilterParse(t *testing.T) {
	// Reusable input operators
	sourceInput := map[string]any{
		"Operator": "Source",
		"Source": map[string]any{
			"file-name": "country_full.csv",
			"local":     false,
		},
	}

	projectInput := map[string]any{
		"Operator": "Project",
		"Project": map[string]any{
			"input": sourceInput,
			"expressions": []map[string]any{
				{
					"expr_type": "ColumnResolve",
					"name":      "name",
				},
				{
					"expr_type": "ColumnResolve",
					"name":      "country-code",
				},
			},
		},
	}

	t.Run("filter with source input", func(t *testing.T) {
		filterTestID := "filter with source test"
		lpMetaData := NewPlanMetaData(filterTestID)

		tests := []struct {
			testName    string
			logicalPlan jsonOBJ
			expectError bool
		}{
			{
				testName: "basic filter with binary expression (column > literal)",
				logicalPlan: map[string]any{
					"input": sourceInput,
					"expression": map[string]any{
						"expr_type": "BinaryExpr",
						"op":        "GreaterThan",
						"left": map[string]any{
							"expr_type": "ColumnResolve",
							"name":      "region",
						},
						"right": map[string]any{
							"expr_type": "LiteralResolve",
							"value":     "Africa",
							"lit_type":  "string",
						},
					},
				},
				expectError: false,
			},
			{
				testName: "filter with column resolve expression",
				logicalPlan: map[string]any{
					"input": sourceInput,
					"expression": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
				},
				expectError: false,
			},
			{
				testName: "filter missing expression field (should fail)",
				logicalPlan: map[string]any{
					"input": sourceInput,
				},
				expectError: true,
			},
			{
				testName: "filter missing input field (should fail)",
				logicalPlan: map[string]any{
					"expression": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
				},
				expectError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.testName, func(t *testing.T) {
				filter, err := parseFilter(tt.logicalPlan, lpMetaData)
				if (err != nil) != tt.expectError {
					t.Errorf("parseFilter() error = %v, expectError = %v", err, tt.expectError)
					return
				}
				if !tt.expectError && filter == nil {
					t.Errorf("parseFilter() returned nil filter when error was nil")
				}
			})
		}
	})

	t.Run("filter with project input", func(t *testing.T) {
		filterTestID := "filter with project test"
		lpMetaData := NewPlanMetaData(filterTestID)

		tests := []struct {
			testName    string
			logicalPlan jsonOBJ
			expectError bool
		}{
			{
				testName: "filter projected columns with binary expression",
				logicalPlan: map[string]any{
					"input": projectInput,
					"expression": map[string]any{
						"expr_type": "BinaryExpr",
						"op":        "GreaterThan",
						"left": map[string]any{
							"expr_type": "ColumnResolve",
							"name":      "country-code",
						},
						"right": map[string]any{
							"expr_type": "LiteralResolve",
							"value":     50,
							"lit_type":  "int",
						},
					},
				},
				expectError: false,
			},
			{
				testName: "filter with complex nested expression",
				logicalPlan: map[string]any{
					"input": projectInput,
					"expression": map[string]any{
						"expr_type": "BinaryExpr",
						"op":        "And",
						"left": map[string]any{
							"expr_type": "BinaryExpr",
							"op":        "Equal",
							"left": map[string]any{
								"expr_type": "ColumnResolve",
								"name":      "name",
							},
							"right": map[string]any{
								"expr_type": "LiteralResolve",
								"value":     "Canada",
								"lit_type":  "string",
							},
						},
						"right": map[string]any{
							"expr_type": "BinaryExpr",
							"op":        "NotEqual",
							"left": map[string]any{
								"expr_type": "ColumnResolve",
								"name":      "name",
							},
							"right": map[string]any{
								"expr_type": "LiteralResolve",
								"value":     "",
								"lit_type":  "string",
							},
						},
					},
				},
				expectError: false,
			},
			{
				testName: "filter with invalid expression type (should fail)",
				logicalPlan: map[string]any{
					"input": projectInput,
					"expression": map[string]any{
						"expr_type": "UnknownType",
						"value":     "invalid",
					},
				},
				expectError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.testName, func(t *testing.T) {
				filter, err := parseFilter(tt.logicalPlan, lpMetaData)
				if (err != nil) != tt.expectError {
					t.Errorf("parseFilter() error = %v, expectError = %v", err, tt.expectError)
					return
				}
				if !tt.expectError && filter == nil {
					t.Errorf("parseFilter() returned nil filter when error was nil")
				}
			})
		}
	})
}

func TestDistinctParse(t *testing.T) {
	// Reusable input operators
	sourceInput := map[string]any{
		"Operator": "Source",
		"Source": map[string]any{
			"file-name": "country_full.csv",
			"local":     false,
		},
	}

	projectInput := map[string]any{
		"Operator": "Project",
		"Project": map[string]any{
			"input": sourceInput,
			"expressions": []map[string]any{
				{
					"expr_type": "ColumnResolve",
					"name":      "name",
				},
				{
					"expr_type": "ColumnResolve",
					"name":      "region",
				},
			},
		},
	}

	distinctTestID := "distinct test"
	lpMetaData := NewPlanMetaData(distinctTestID)

	tests := []struct {
		testName    string
		logicalPlan jsonOBJ
		expectError bool
	}{
		{
			testName: "distinct with single column",
			logicalPlan: map[string]any{
				"input": sourceInput,
				"expressions": []map[string]any{
					{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
				},
			},
			expectError: false,
		},
		{
			testName: "distinct with multiple columns",
			logicalPlan: map[string]any{
				"input": sourceInput,
				"expressions": []map[string]any{
					{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
					{
						"expr_type": "ColumnResolve",
						"name":      "region",
					},
				},
			},
			expectError: false,
		},
		{
			testName: "distinct on project input",
			logicalPlan: map[string]any{
				"input": projectInput,
				"expressions": []map[string]any{
					{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
				},
			},
			expectError: false,
		},
		{
			testName: "distinct missing expressions field (should fail)",
			logicalPlan: map[string]any{
				"input": sourceInput,
			},
			expectError: true,
		},
		{
			testName: "distinct with empty expressions (should fail)",
			logicalPlan: map[string]any{
				"input":       sourceInput,
				"expressions": []map[string]any{},
			},
			expectError: true,
		},
		{
			testName: "distinct missing input field (should fail)",
			logicalPlan: map[string]any{
				"expressions": []map[string]any{
					{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			distinct, err := parseDistinct(tt.logicalPlan, lpMetaData)
			if (err != nil) != tt.expectError {
				t.Errorf("parseDistinct() error = %v, expectError = %v", err, tt.expectError)
				return
			}
			if !tt.expectError && distinct == nil {
				t.Errorf("parseDistinct() returned nil when error was nil")
			}
		})
	}
}

func TestLimitParse(t *testing.T) {
	// Reusable input operators
	sourceInput := map[string]any{
		"Operator": "Source",
		"Source": map[string]any{
			"file-name": "country_full.csv",
			"local":     false,
		},
	}

	projectInput := map[string]any{
		"Operator": "Project",
		"Project": map[string]any{
			"input": sourceInput,
			"expressions": []map[string]any{
				{
					"expr_type": "ColumnResolve",
					"name":      "name",
				},
			},
		},
	}

	limitTestID := "limit test"
	lpMetaData := NewPlanMetaData(limitTestID)

	tests := []struct {
		testName      string
		logicalPlan   jsonOBJ
		expectedLimit int64
		expectError   bool
	}{
		{
			testName: "limit with small value",
			logicalPlan: map[string]any{
				"input": sourceInput,
				"limit": 10,
			},
			expectedLimit: 10,
			expectError:   false,
		},
		{
			testName: "limit with large value",
			logicalPlan: map[string]any{
				"input": sourceInput,
				"limit": 10000,
			},
			expectedLimit: 10000,
			expectError:   false,
		},
		{
			testName: "limit with value thats too large",
			logicalPlan: map[string]any{
				"input": sourceInput,
				"limit": math.MaxUint16 + 100,
			},
			expectedLimit: 1000000,
			expectError:   true,
		},
		{
			testName: "limit on projected input",
			logicalPlan: map[string]any{
				"input": projectInput,
				"limit": 5,
			},
			expectedLimit: 5,
			expectError:   false,
		},
		{
			testName: "limit missing limit field (should fail)",
			logicalPlan: map[string]any{
				"input": sourceInput,
			},
			expectError: true,
		},
		{
			testName: "limit missing input field (should fail)",
			logicalPlan: map[string]any{
				"limit": 10,
			},
			expectError: true,
		},
		{
			testName: "limit with zero value (should fail)",
			logicalPlan: map[string]any{
				"input": sourceInput,
				"limit": 0,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			limit, err := parseLimit(tt.logicalPlan, lpMetaData)
			if (err != nil) != tt.expectError {
				t.Errorf("parseLimit() error = %v, expectError = %v", err, tt.expectError)
				return
			}
			if !tt.expectError {
				if limit == nil {
					t.Errorf("parseLimit() returned nil when error was nil")
					return
				}

				// Verify limit value is set correctly
				if int64(limit.Remaining) != tt.expectedLimit {
					t.Errorf("parseLimit() limit value = %d, expected %d", limit.Remaining, tt.expectedLimit)
				}
			}
		})
	}
}

func TestSortParse(t *testing.T) {
	// Reusable input operators
	sourceInput := map[string]any{
		"Operator": "Source",
		"Source": map[string]any{
			"file-name": "country_full.csv",
			"local":     false,
		},
	}

	projectInput := map[string]any{
		"Operator": "Project",
		"Project": map[string]any{
			"input": sourceInput,
			"expressions": []map[string]any{
				{
					"expr_type": "ColumnResolve",
					"name":      "name",
				},
			},
		},
	}

	sortTestID := "sort test"
	lpMetaData := NewPlanMetaData(sortTestID)

	tests := []struct {
		testName    string
		logicalPlan jsonOBJ
		expectError bool
	}{
		{
			testName: "sort single column ascending",
			logicalPlan: map[string]any{
				"input": sourceInput,
				"by": []map[string]any{
					{
						"Expr": map[string]any{
							"expr_type": "ColumnResolve",
							"name":      "name",
						},
						"asc": true,
					},
				},
			},
			expectError: false,
		},
		{
			testName: "sort single column descending",
			logicalPlan: map[string]any{
				"input": sourceInput,
				"by": []map[string]any{
					{
						"Expr": map[string]any{
							"expr_type": "ColumnResolve",
							"name":      "name",
						},
						"asc": false,
					},
				},
			},
			expectError: false,
		},
		{
			testName: "sort multiple columns",
			logicalPlan: map[string]any{
				"input": projectInput,
				"by": []map[string]any{
					{
						"Expr": map[string]any{
							"expr_type": "ColumnResolve",
							"name":      "name",
						},
						"asc": true,
					},
				},
			},
			expectError: false,
		},
		{
			testName: "sort missing by field (should fail)",
			logicalPlan: map[string]any{
				"input": sourceInput,
			},
			expectError: true,
		},
		{
			testName: "sort missing input field (should fail)",
			logicalPlan: map[string]any{
				"by": []map[string]any{
					{
						"Expr": map[string]any{
							"expr_type": "ColumnResolve",
							"name":      "name",
						},
						"asc": true,
					},
				},
			},
			expectError: true,
		},
		{
			testName: "sort with empty by array (should fail)",
			logicalPlan: map[string]any{
				"input": sourceInput,
				"by":    []map[string]any{},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			sort, err := parseSort(tt.logicalPlan, lpMetaData)
			if (err != nil) != tt.expectError {
				t.Errorf("parseSort() error = %v, expectError = %v", err, tt.expectError)
				return
			}
			if !tt.expectError && sort == nil {
				t.Errorf("parseSort() returned nil when error was nil")
			}
		})
	}
}

func TestAggregateParse(t *testing.T) {
	// Reusable input operators
	sourceInput := map[string]any{
		"Operator": "Source",
		"Source": map[string]any{
			"file-name": "country_full.csv",
			"local":     false,
		},
	}

	projectNumericInput := map[string]any{
		"Operator": "Project",
		"Project": map[string]any{
			"input": sourceInput,
			"expressions": []map[string]any{
				{
					"expr_type": "ColumnResolve",
					"name":      "country-code",
				},
				{
					"expr_type": "ColumnResolve",
					"name":      "region-code",
				},
			},
		},
	}

	projectStringInput := map[string]any{
		"Operator": "Project",
		"Project": map[string]any{
			"input": sourceInput,
			"expressions": []map[string]any{
				{
					"expr_type": "ColumnResolve",
					"name":      "name",
				},
				{
					"expr_type": "ColumnResolve",
					"name":      "region",
				},
			},
		},
	}

	aggregateTestID := "aggregate test"
	lpMetaData := NewPlanMetaData(aggregateTestID)

	tests := []struct {
		testName    string
		logicalPlan jsonOBJ
		expectError bool
	}{
		{
			testName: "aggregate Sum on numeric column",
			logicalPlan: map[string]any{
				"input":    sourceInput,
				"function": "Sum",
				"column": map[string]any{
					"expr_type": "ColumnResolve",
					"name":      "country-code",
				},
				"alias": "sum_country_code",
			},
			expectError: false,
		},
		{
			testName: "aggregate Count on string column",
			logicalPlan: map[string]any{
				"input":    sourceInput,
				"function": "Count",
				"column": map[string]any{
					"expr_type": "ColumnResolve",
					"name":      "name",
				},
				"alias": "count_countries",
			},
			expectError: false,
		},
		{
			testName: "aggregate Avg on numeric column",
			logicalPlan: map[string]any{
				"input":    sourceInput,
				"function": "Avg",
				"column": map[string]any{
					"expr_type": "ColumnResolve",
					"name":      "region-code",
				},
				"alias": "avg_region_code",
			},
			expectError: false,
		},
		{
			testName: "aggregate Min on numeric column",
			logicalPlan: map[string]any{
				"input":    projectNumericInput,
				"function": "Min",
				"column": map[string]any{
					"expr_type": "ColumnResolve",
					"name":      "country-code",
				},
				"alias": "min_country_code",
			},
			expectError: false,
		},
		{
			testName: "aggregate Max on string column",
			logicalPlan: map[string]any{
				"input":    projectStringInput,
				"function": "Max",
				"column": map[string]any{
					"expr_type": "ColumnResolve",
					"name":      "region",
				},
				"alias": "max_region",
			},
			expectError: false,
		},
		{
			testName: "aggregate missing function field (should fail)",
			logicalPlan: map[string]any{
				"input": sourceInput,
				"column": map[string]any{
					"expr_type": "ColumnResolve",
					"name":      "name",
				},
				"alias": "count_name",
			},
			expectError: true,
		},
		{
			testName: "aggregate missing column field (should fail)",
			logicalPlan: map[string]any{
				"input":    sourceInput,
				"function": "Sum",
				"alias":    "sum_code",
			},
			expectError: true,
		},
		{
			testName: "aggregate missing input field (should fail)",
			logicalPlan: map[string]any{
				"function": "Sum",
				"column": map[string]any{
					"expr_type": "ColumnResolve",
					"name":      "country-code",
				},
				"alias": "sum_code",
			},
			expectError: true,
		},
		{
			testName: "aggregate missing alias field (should fail)",
			logicalPlan: map[string]any{
				"input":    sourceInput,
				"function": "Sum",
				"column": map[string]any{
					"expr_type": "ColumnResolve",
					"name":      "country-code",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			aggregate, err := parseSingleAggr(tt.logicalPlan, lpMetaData)
			if (err != nil) != tt.expectError {
				t.Errorf("parseAggregate() error = %v, expectError = %v", err, tt.expectError)
				return
			}
			if !tt.expectError && aggregate == nil {
				t.Errorf("parseAggregate() returned nil when error was nil")
			}
		})
	}
}

func TestHavingParse(t *testing.T) {
	// Reusable input operators
	sourceInput := map[string]any{
		"Operator": "Source",
		"Source": map[string]any{
			"file-name": "country_full.csv",
			"local":     false,
		},
	}

	projectInput := map[string]any{
		"Operator": "Project",
		"Project": map[string]any{
			"input": sourceInput,
			"expressions": []map[string]any{
				{
					"expr_type": "ColumnResolve",
					"name":      "name",
				},
			},
		},
	}

	havingTestID := "having test"
	lpMetaData := NewPlanMetaData(havingTestID)

	tests := []struct {
		testName    string
		logicalPlan jsonOBJ
		expectError bool
	}{
		{
			testName: "having with simple equality expression",
			logicalPlan: map[string]any{
				"input": sourceInput,
				"expression": map[string]any{
					"expr_type": "BinaryExpr",
					"op":        "Equal",
					"left": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
					"right": map[string]any{
						"expr_type": "LiteralResolve",
						"value":     "Canada",
						"lit_type":  "string",
					},
				},
			},
			expectError: false,
		},
		{
			testName: "having with complex AND expression",
			logicalPlan: map[string]any{
				"input": projectInput,
				"expression": map[string]any{
					"expr_type": "BinaryExpr",
					"op":        "And",
					"left": map[string]any{
						"expr_type": "BinaryExpr",
						"op":        "Equal",
						"left": map[string]any{
							"expr_type": "ColumnResolve",
							"name":      "name",
						},
						"right": map[string]any{
							"expr_type": "LiteralResolve",
							"value":     "Canada",
							"lit_type":  "string",
						},
					},
					"right": map[string]any{
						"expr_type": "BinaryExpr",
						"op":        "NotEqual",
						"left": map[string]any{
							"expr_type": "ColumnResolve",
							"name":      "name",
						},
						"right": map[string]any{
							"expr_type": "LiteralResolve",
							"value":     "",
							"lit_type":  "string",
						},
					},
				},
			},
			expectError: false,
		},
		{
			testName: "having missing expression field (should fail)",
			logicalPlan: map[string]any{
				"input": sourceInput,
			},
			expectError: true,
		},
		{
			testName: "having missing input field (should fail)",
			logicalPlan: map[string]any{
				"expression": map[string]any{
					"expr_type": "BinaryExpr",
					"op":        "Equal",
					"left": map[string]any{
						"expr_type": "ColumnResolve",
						"name":      "name",
					},
					"right": map[string]any{
						"expr_type": "LiteralResolve",
						"value":     "Canada",
						"lit_type":  "string",
					},
				},
			},
			expectError: true,
		},
		{
			testName: "having with OR expression",
			logicalPlan: map[string]any{
				"input": sourceInput,
				"expression": map[string]any{
					"expr_type": "BinaryExpr",
					"op":        "Or",
					"left": map[string]any{
						"expr_type": "BinaryExpr",
						"op":        "Equal",
						"left": map[string]any{
							"expr_type": "ColumnResolve",
							"name":      "name",
						},
						"right": map[string]any{
							"expr_type": "LiteralResolve",
							"value":     "USA",
							"lit_type":  "string",
						},
					},
					"right": map[string]any{
						"expr_type": "BinaryExpr",
						"op":        "Equal",
						"left": map[string]any{
							"expr_type": "ColumnResolve",
							"name":      "name",
						},
						"right": map[string]any{
							"expr_type": "LiteralResolve",
							"value":     "Canada",
							"lit_type":  "string",
						},
					},
				},
			},
			expectError: false,
		},
		{
			testName: "having with literal only expression (should fail)",
			logicalPlan: map[string]any{
				"input": sourceInput,
				"expression": map[string]any{
					"expr_type": "LiteralResolve",
					"value":     "Canada",
					"lit_type":  "string",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			having, err := parseHaving(tt.logicalPlan, lpMetaData)
			if (err != nil) != tt.expectError {
				t.Errorf("parseHaving() error = %v, expectError = %v", err, tt.expectError)
				return
			}
			if !tt.expectError && having == nil {
				t.Errorf("parseHaving() returned nil when error was nil")
			}
		})
	}
}

func TestSourceParse(t *testing.T) {
	t.Run("source with local CSV", func(t *testing.T) {
		sourceTestID := "source local csv test"
		lpMetaData := NewPlanMetaData(sourceTestID)

		tests := []struct {
			testName    string
			logicalPlan jsonOBJ
			expectError bool
		}{
			{
				testName: "local CSV file",
				logicalPlan: map[string]any{
					"file-name": "country_full.csv",
					"local":     true,
				},
				expectError: false,
			},
			{
				testName: "local CSV with various extension",
				logicalPlan: map[string]any{
					"file-name": "data.csv",
					"local":     true,
				},
				expectError: true,
			},
			{
				testName: "missing file-name field (should fail)",
				logicalPlan: map[string]any{
					"local": true,
				},
				expectError: true,
			},
			{
				testName: "invalid file extension (should fail)",
				logicalPlan: map[string]any{
					"file-name": "data.txt",
					"local":     true,
				},
				expectError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.testName, func(t *testing.T) {
				source, err := parseSource(tt.logicalPlan, lpMetaData)
				if (err != nil) != tt.expectError {
					t.Errorf("parseSource() error = %v, expectError = %v", err, tt.expectError)
					return
				}
				if !tt.expectError && source == nil {
					t.Errorf("parseSource() returned nil when error was nil")
				}
			})
		}
	})

	t.Run("source with remote files", func(t *testing.T) {
		sourceTestID := "source remote test"
		lpMetaData := NewPlanMetaData(sourceTestID)

		tests := []struct {
			testName    string
			logicalPlan jsonOBJ
			expectError bool
		}{
			{
				testName: "remote CSV file",
				logicalPlan: map[string]any{
					"file-name": "country_full.csv",
					"local":     false,
				},
				expectError: false,
			},
			{
				testName: "remote parquet file",
				logicalPlan: map[string]any{
					"file-name": "userdata.parquet",
					"local":     false,
				},
				expectError: false,
			},
			{
				testName: "remote file with unsupported extension (should fail)",
				logicalPlan: map[string]any{
					"file-name": "s3://bucket/data.json",
					"local":     false,
				},
				expectError: true,
			},
			{
				testName: "missing local field (should fail)",
				logicalPlan: map[string]any{
					"file-name": "data.csv",
				},
				expectError: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.testName, func(t *testing.T) {
				source, err := parseSource(tt.logicalPlan, lpMetaData)
				if (err != nil) != tt.expectError {
					t.Errorf("parseSource() error = %v, expectError = %v", err, tt.expectError)
					return
				}
				if !tt.expectError && source == nil {
					t.Errorf("parseSource() returned nil when error was nil")
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
			name:       "int type correct",
			fields:     []string{"count"},
			fieldTypes: []string{"int"},
			obj:        jsonOBJ{"count": 10},
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
