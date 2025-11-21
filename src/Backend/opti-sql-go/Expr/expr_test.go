package Expr

import (
	"log"
	"opti-sql-go/operators"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// (removed helper that constructed a record builder - not needed in Expr tests)

// Do not change this
// generateTestColumns returns a RecordBatch containing the first 4 rows of the
// commonly used test data. Returning a RecordBatch allows Expr tests to avoid
// depending on the `project` package (no import cycle).
func generateTestColumns() *operators.RecordBatch {
	mem := memory.DefaultAllocator

	// id
	idB := array.NewInt32Builder(mem)
	defer idB.Release()
	idB.AppendValues([]int32{1, 2, 3, 4}, nil)
	idArr := idB.NewArray()

	// name
	nameB := array.NewStringBuilder(mem)
	defer nameB.Release()
	nameB.AppendValues([]string{"Alice", "Bob", "Charlie", "David"}, nil)
	nameArr := nameB.NewArray()

	// age
	ageB := array.NewInt32Builder(mem)
	defer ageB.Release()
	ageB.AppendValues([]int32{28, 34, 45, 22}, nil)
	ageArr := ageB.NewArray()

	// salary
	salB := array.NewFloat64Builder(mem)
	defer salB.Release()
	salB.AppendValues([]float64{70000.0, 82000.5, 54000.0, 91000.0}, nil)
	salArr := salB.NewArray()

	// is_active
	actB := array.NewBooleanBuilder(mem)
	defer actB.Release()
	actB.AppendValues([]bool{true, false, true, true}, nil)
	actArr := actB.NewArray()

	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "salary", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "is_active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)

	cols := []arrow.Array{idArr, nameArr, ageArr, salArr, actArr}
	return &operators.RecordBatch{Schema: schema, Columns: cols, RowCount: 4}
}
func GenerateColumsNull() arrow.Array {
	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendNull()
	b.Append("first-value")
	b.AppendNull()
	b.Append("second-value")

	return b.NewArray()
}

func TestAliasExpr(t *testing.T) {
	rc := generateTestColumns() // 4
	// alias should return the underlying expression with a new name
	// the name swap out occures in project so for now just validate evaluation is as expected
	t.Run("Alias on Name", func(t *testing.T) {
		a := Alias{
			Expr: &ColumnResolve{Name: "name"},
			Name: "employee_name",
		}
		_ = a.String()
		arr, err := EvalExpression(&a, rc)
		if err != nil {
			t.Fatalf("failed to evaluate alias expression: %v", err)
		}
		expectedArr := []string{"Alice", "Bob", "Charlie", "David"}
		if len(expectedArr) != arr.Len() {
			t.Fatalf("expected length %d, got %d", len(expectedArr), arr.Len())
		}
		for i := 0; i < arr.Len(); i++ {
			expected := expectedArr[i]
			actual := arr.(*array.String).Value(i)
			if expected != actual {
				t.Fatalf("at index %d, expected %s, got %s", i, expected, actual)
			}
		}

	})
	t.Run("Alias on Salary", func(t *testing.T) {
		a := Alias{
			Expr: &ColumnResolve{Name: "salary"},
			Name: "employee_salary",
		}
		_ = a.String()
		arr, err := EvalExpression(&a, rc)
		if err != nil {
			t.Fatalf("failed to evaluate alias expression: %v", err)
		}
		expectedArr := []float64{70000.0, 82000.5, 54000.0, 91000.0}
		if len(expectedArr) != arr.Len() {
			t.Fatalf("expected length %d, got %d", len(expectedArr), arr.Len())
		}
		for i := 0; i < arr.Len(); i++ {
			expected := expectedArr[i]
			actual := arr.(*array.Float64).Value(i)
			if expected != actual {
				t.Fatalf("at index %d, expected %f, got %f", i, expected, actual)
			}
		}

	})
	t.Run("Alias on is_active", func(t *testing.T) {
		a := Alias{
			Expr: &ColumnResolve{Name: "is_active"},
			Name: "active_status",
		}
		_ = a.String()
		arr, err := EvalExpression(&a, rc)
		if err != nil {
			t.Fatalf("failed to evaluate alias expression: %v", err)
		}
		expectedArr := []bool{true, false, true, true}
		if len(expectedArr) != arr.Len() {
			t.Fatalf("expected length %d, got %d", len(expectedArr), arr.Len())
		}
		for i := 0; i < arr.Len(); i++ {
			expected := expectedArr[i]
			actual := arr.(*array.Boolean).Value(i)
			if expected != actual {
				t.Fatalf("at index %d, expected %v, got %v", i, expected, actual)
			}
		}
	})
	// interface validation
	a := Alias{Name: "New_Name"}
	a.ExprNode()
	t.Logf("%s", a.String())
}

func TestColumnResolve(t *testing.T) {
	rc := generateTestColumns() //
	t.Run("ColumnResolve on age", func(t *testing.T) {
		cr := ColumnResolve{Name: "age"}
		arr, err := EvalExpression(&cr, rc)
		if err != nil {
			t.Fatalf("failed to evaluate column resolve expression: %v", err)
		}
		expectedArr := []int32{28, 34, 45, 22}
		if len(expectedArr) != arr.Len() {
			t.Fatalf("expected length %d, got %d", len(expectedArr), arr.Len())
		}
		for i := 0; i < arr.Len(); i++ {
			expected := expectedArr[i]
			actual := arr.(*array.Int32).Value(i)
			if expected != actual {
				t.Fatalf("at index %d, expected %d, got %d", i, expected, actual)
			}
		}
	})
	t.Run("ColumnResolve on ID", func(t *testing.T) {
		cr := ColumnResolve{Name: "id"}
		arr, err := EvalExpression(&cr, rc)
		if err != nil {
			t.Fatalf("failed to evaluate column resolve expression: %v", err)
		}
		expectedArr := []int32{1, 2, 3, 4}
		if len(expectedArr) != arr.Len() {
			t.Fatalf("expected length %d, got %d", len(expectedArr), arr.Len())
		}
		for i := 0; i < arr.Len(); i++ {
			expected := expectedArr[i]
			actual := arr.(*array.Int32).Value(i)
			if expected != actual {
				t.Fatalf("at index %d, expected %d, got %d", i, expected, actual)
			}
		}
	})
	t.Run("ColumnResolve on non-existant column", func(t *testing.T) {
		cr := ColumnResolve{Name: "doesnt Exist"}
		_, err := EvalExpression(&cr, rc)
		if err == nil {
			t.Fatalf("expected error for non existant column")
		}
	})
	// interface Validation
	cr := ColumnResolve{Name: "--"}
	cr.ExprNode()
	t.Logf("%s\n", cr.String())
}

func TestLiteralResolve(t *testing.T) {
	t.Run("EvalLiteral", func(t *testing.T) {
		rc := generateTestColumns() //

		// -------------------------
		// BOOLEAN
		// -------------------------
		t.Run("BOOL", func(t *testing.T) {
			lit := &LiteralResolve{
				Type:  arrow.FixedWidthTypes.Boolean,
				Value: true,
			}
			arr, err := EvalLiteral(lit, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			b := arr.(*array.Boolean)
			for i := 0; i < arr.Len(); i++ {
				if !b.Value(i) {
					t.Fatalf("expected true at index %d", i)
				}
			}
		})
		// -------------------------
		// Int8
		// -------------------------
		t.Run("INT8", func(t *testing.T) {
			lit := &LiteralResolve{
				Type:  arrow.PrimitiveTypes.Int8,
				Value: int8(-5),
			}
			arr, err := EvalLiteral(lit, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			out := arr.(*array.Int8)
			if out.Len() != 4 {
				t.Fatalf("expected 4, got %d", out.Len())
			}
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != -5 {
					t.Fatalf("expected -5 at %d, got %d", i, out.Value(i))
				}
			}
		})

		// -------------------------
		// Uint8
		// -------------------------
		t.Run("UINT8", func(t *testing.T) {
			lit := &LiteralResolve{
				Type:  arrow.PrimitiveTypes.Uint8,
				Value: uint8(7),
			}
			arr, err := EvalLiteral(lit, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			out := arr.(*array.Uint8)
			if out.Len() != 4 {
				t.Fatalf("expected 4, got %d", out.Len())
			}
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != 7 {
					t.Fatalf("expected 7 at %d, got %d", i, out.Value(i))
				}
			}
		})
		// -------------------------
		// int16
		// -------------------------

		t.Run("INT16", func(t *testing.T) {
			lit := &LiteralResolve{
				Type:  arrow.PrimitiveTypes.Int16,
				Value: int16(1234),
			}
			arr, err := EvalLiteral(lit, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			out := arr.(*array.Int16)
			if out.Len() != 4 {
				t.Fatalf("expected 4, got %d", out.Len())
			}
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != 1234 {
					t.Fatalf("expected 1234 at %d, got %d", i, out.Value(i))
				}
			}
		})
		// -------------------------
		// Uint16
		// -------------------------
		t.Run("UINT16", func(t *testing.T) {
			lit := &LiteralResolve{
				Type:  arrow.PrimitiveTypes.Uint16,
				Value: uint16(60000),
			}
			arr, err := EvalLiteral(lit, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			out := arr.(*array.Uint16)
			if out.Len() != 4 {
				t.Fatalf("expected 4, got %d", out.Len())
			}
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != 60000 {
					t.Fatalf("expected 60000 at %d, got %d", i, out.Value(i))
				}
			}
		})

		// -------------------------
		// INT32
		// -------------------------
		t.Run("INT32", func(t *testing.T) {
			lit := &LiteralResolve{
				Type:  arrow.PrimitiveTypes.Int32,
				Value: int32(99),
			}
			arr, err := EvalLiteral(lit, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			col := arr.(*array.Int32)
			for i := 0; i < arr.Len(); i++ {
				if col.Value(i) != 99 {
					t.Fatalf("expected 99, got %d", col.Value(i))
				}
			}
		})
		// -------------------------
		// UINT32
		// -------------------------
		t.Run("UINT32", func(t *testing.T) {
			lit := &LiteralResolve{
				Type:  arrow.PrimitiveTypes.Uint32,
				Value: uint32(4000000000),
			}
			arr, err := EvalLiteral(lit, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			out := arr.(*array.Uint32)
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != 4000000000 {
					t.Fatalf("expected 4000000000 at %d, got %d", i, out.Value(i))
				}
			}
		})

		// -------------------------
		// INT64
		// -------------------------
		t.Run("INT64", func(t *testing.T) {
			lit := &LiteralResolve{
				Type:  arrow.PrimitiveTypes.Int64,
				Value: int64(123456),
			}
			arr, err := EvalLiteral(lit, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			col := arr.(*array.Int64)
			for i := 0; i < arr.Len(); i++ {
				if col.Value(i) != 123456 {
					t.Fatalf("expected 123456, got %d", col.Value(i))
				}
			}
		})
		// -------------------------
		// UINT64
		// -------------------------
		t.Run("UINT64", func(t *testing.T) {
			lit := &LiteralResolve{
				Type:  arrow.PrimitiveTypes.Uint64,
				Value: uint64(9999999999),
			}
			arr, err := EvalLiteral(lit, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			out := arr.(*array.Uint64)
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != 9999999999 {
					t.Fatalf("expected 9999999999 at %d, got %d", i, out.Value(i))
				}
			}
		})
		// -------------------------
		// FLOAT32
		// -------------------------

		t.Run("FLOAT32", func(t *testing.T) {
			lit := &LiteralResolve{
				Type:  arrow.PrimitiveTypes.Float32,
				Value: float32(3.14),
			}
			arr, err := EvalLiteral(lit, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			out := arr.(*array.Float32)
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != float32(3.14) {
					t.Fatalf("expected 3.14 at %d, got %f", i, out.Value(i))
				}
			}
		})

		// -------------------------
		// FLOAT64
		// -------------------------
		t.Run("FLOAT64", func(t *testing.T) {
			lit := &LiteralResolve{
				Type:  arrow.PrimitiveTypes.Float64,
				Value: float64(3.14),
			}
			arr, err := EvalLiteral(lit, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			col := arr.(*array.Float64)
			for i := 0; i < arr.Len(); i++ {
				if col.Value(i) != 3.14 {
					t.Fatalf("expected 3.14, got %f", col.Value(i))
				}
			}
		})

		// -------------------------
		// STRING
		// -------------------------
		t.Run("STRING", func(t *testing.T) {
			lit := &LiteralResolve{
				Type:  arrow.BinaryTypes.String,
				Value: "hello",
			}
			arr, err := EvalLiteral(lit, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			col := arr.(*array.String)
			for i := 0; i < arr.Len(); i++ {
				if col.Value(i) != "hello" {
					t.Fatalf("expected 'hello', got '%s'", col.Value(i))
				}
			}
		})

		// -------------------------
		// BINARY
		// -------------------------
		t.Run("BINARY", func(t *testing.T) {
			bval := []byte{1, 2, 3}
			lit := &LiteralResolve{
				Type:  arrow.BinaryTypes.Binary,
				Value: bval,
			}
			arr, err := EvalLiteral(lit, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			col := arr.(*array.Binary)
			for i := 0; i < arr.Len(); i++ {
				v := col.Value(i)
				if string(v) != string(bval) {
					t.Fatalf("expected %v, got %v", bval, v)
				}
			}
		})

		// -------------------------
		// ERROR CASE
		// -------------------------
		t.Run("UnsupportedType", func(t *testing.T) {
			lit := &LiteralResolve{
				Type:  arrow.FixedWidthTypes.Duration_s, // something you did NOT implement
				Value: int64(10),
			}
			_, err := EvalLiteral(lit, rc)
			if err == nil {
				t.Fatalf("expected error for unsupported type, got nil")
			}
		})

		// -------------------------
		// Validate .String() and .ExprNode()
		// -------------------------
		t.Run("Interface methods", func(t *testing.T) {
			lit := &LiteralResolve{
				Type:  arrow.PrimitiveTypes.Int32,
				Value: int32(1),
			}

			// Just call, like your other tests
			t.Logf("%s\n", lit.String())
			lit.ExprNode()
		})
	})

}

func TestBinaryExpr(t *testing.T) {
	t.Run("BinaryExpr Arithmetic", func(t *testing.T) {
		rc := generateTestColumns() //4

		makeLit := func(v int32) Expression {
			return &LiteralResolve{
				Type:  arrow.PrimitiveTypes.Int32,
				Value: v,
			}
		}

		t.Run("Addition", func(t *testing.T) {
			b := &BinaryExpr{
				Left:  makeLit(10),
				Op:    Addition,
				Right: makeLit(5),
			}

			arr, err := EvalBinary(b, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			out := arr.(*array.Int32)
			for i := 0; i < out.Len(); i++ {
				got := out.Value(i)
				if got != 15 {
					t.Fatalf("expected 15, got %d (index %d)", got, i)
				}
			}
		})

		t.Run("Subtraction", func(t *testing.T) {
			b := &BinaryExpr{
				Left:  makeLit(20),
				Op:    Subtraction,
				Right: makeLit(3),
			}

			arr, err := EvalExpression(b, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			out := arr.(*array.Int32)
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != 17 {
					t.Fatalf("expected 17, got %d", out.Value(i))
				}
			}
		})

		t.Run("Multiplication", func(t *testing.T) {
			b := &BinaryExpr{
				Left:  makeLit(7),
				Op:    Multiplication,
				Right: makeLit(6),
			}

			arr, err := EvalExpression(b, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			out := arr.(*array.Int32)
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != 42 {
					t.Fatalf("expected 42, got %d", out.Value(i))
				}
			}
		})

		t.Run("Division", func(t *testing.T) {
			b := &BinaryExpr{
				Left:  makeLit(20),
				Op:    Division,
				Right: makeLit(4),
			}

			arr, err := EvalExpression(b, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			out := arr.(*array.Int32)
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != 5 {
					t.Fatalf("expected 5, got %d", out.Value(i))
				}
			}
		})

		t.Run("UnsupportedOperator", func(t *testing.T) {
			b := &BinaryExpr{
				Left:  makeLit(1),
				Op:    9999, // unsupported
				Right: makeLit(1),
			}

			_, err := EvalExpression(b, rc)
			if err == nil {
				t.Fatalf("expected error for unsupported operator, got nil")
			}
		})
		t.Run("Invalid datum", func(t *testing.T) {
			datum := compute.NewDatum(4)
			t.Logf("datum:%v\n", datum)
			_, err := unpackDatum(datum)
			if err == nil {
				t.Fatalf("expected error for invalid datum, got nil")
			}
		})

		// -- interface / string validation ---------------------------------------

		be := &BinaryExpr{
			Left:  makeLit(1),
			Op:    Addition,
			Right: makeLit(2),
		}
		log.Printf("%s", be.String())
		be.ExprNode()

		t.Logf("BinaryExpr String(): %s\n", be.String())
	})

}

func TestScalarFunctions(t *testing.T) {
	t.Run("ScalarFunction", func(t *testing.T) {

		rc := generateTestColumns() //4

		// Utility literal helper for ints
		makeInt := func(v int32) Expression {
			return &LiteralResolve{
				Type:  arrow.PrimitiveTypes.Int32,
				Value: v,
			}
		}

		// Utility literal helper for strings
		makeStr := func(v string) Expression {
			return &LiteralResolve{
				Type:  arrow.BinaryTypes.String,
				Value: v,
			}
		}

		// -------------------------
		// UPPER
		// -------------------------
		t.Run("Upper", func(t *testing.T) {
			sf := &ScalarFunction{
				Function:  Upper,
				Arguments: makeStr("hello"),
			}

			arr, err := EvalExpression(sf, rc)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}

			out := arr.(*array.String)
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != "HELLO" {
					t.Fatalf("expected HELLO, got %s", out.Value(i))
				}
			}
		})

		// -------------------------
		// LOWER
		// -------------------------
		t.Run("Lower", func(t *testing.T) {
			sf := &ScalarFunction{
				Function:  Lower,
				Arguments: makeStr("HeLLo"),
			}

			arr, err := EvalExpression(sf, rc)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}

			out := arr.(*array.String)
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != "hello" {
					t.Fatalf("expected hello, got %s", out.Value(i))
				}
			}
		})

		// -------------------------
		// ABS
		// -------------------------
		t.Run("Abs", func(t *testing.T) {
			sf := &ScalarFunction{
				Function:  Abs,
				Arguments: makeInt(-9),
			}

			arr, err := EvalExpression(sf, rc)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}

			out := arr.(*array.Int32)
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != 9 {
					t.Fatalf("expected 9, got %d", out.Value(i))
				}
			}
		})

		// -------------------------
		// ROUND
		// -------------------------
		t.Run("Round", func(t *testing.T) {
			sf := &ScalarFunction{
				Function: Round,
				Arguments: &LiteralResolve{
					Type:  arrow.PrimitiveTypes.Float64,
					Value: 3.67,
				},
			}

			arr, err := EvalExpression(sf, rc)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}

			out := arr.(*array.Float64)
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != 4 {
					t.Fatalf("expected 4, got %f", out.Value(i))
				}
			}
		})

		// -------------------------
		// TYPE ERROR for UPPER
		// -------------------------
		t.Run("UpperTypeError", func(t *testing.T) {
			sf := &ScalarFunction{
				Function:  Upper,
				Arguments: makeInt(99), // not a string
			}

			_, err := EvalExpression(sf, rc)
			if err == nil {
				t.Fatalf("expected type error, got nil")
			}
		})
		t.Run("LowerTypeError", func(t *testing.T) {
			sf := &ScalarFunction{
				Function:  Lower,
				Arguments: makeInt(99), // not a string
			}

			_, err := EvalExpression(sf, rc)
			if err == nil {
				t.Fatalf("expected type error, got nil")
			}
		})
		t.Run("Upper with Nulls", func(t *testing.T) {
			nullArr := GenerateColumsNull()
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "col1", Type: arrow.BinaryTypes.String, Nullable: true},
			}, nil)
			defer nullArr.Release()
			cr := ColumnResolve{Name: "col1"}
			sf := &ScalarFunction{
				Function:  Upper,
				Arguments: &cr,
			}
			a, err := EvalExpression(sf, &operators.RecordBatch{Schema: schema, Columns: []arrow.Array{nullArr}})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			t.Logf("array:%v", a)

		})
		t.Run("Lower with Nulls", func(t *testing.T) {
			nullArr := GenerateColumsNull()
			schema := arrow.NewSchema([]arrow.Field{
				{Name: "col1", Type: arrow.BinaryTypes.String, Nullable: true},
			}, nil)
			defer nullArr.Release()
			cr := ColumnResolve{Name: "col1"}
			sf := &ScalarFunction{
				Function:  Lower,
				Arguments: &cr,
			}
			a, err := EvalExpression(sf, &operators.RecordBatch{Schema: schema, Columns: []arrow.Array{nullArr}})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			t.Logf("array:%v", a)

		})
		// -------------------------
		// Unsupported function
		// -------------------------
		t.Run("UnsupportedFunction", func(t *testing.T) {
			sf := &ScalarFunction{
				Function:  999,
				Arguments: makeStr("hi"),
			}

			_, err := EvalExpression(sf, rc)
			if err == nil {
				t.Fatalf("expected unsupported function error, got nil")
			}
		})

		// ----------------------------------------
		// INTERFACE VALIDATION
		// ----------------------------------------
		s := &ScalarFunction{
			Function:  Upper,
			Arguments: makeStr("ok"),
		}

		// Should not panic
		s.ExprNode()

		// Print the string representation
		t.Logf("%s\n", s.String())
	})

}

func TestCastResolve(t *testing.T) {
	t.Run("CastExpr", func(t *testing.T) {

		rc := generateTestColumns() //4

		// ---- Helpers -----
		makeLitInt := func(v int32) Expression {
			return &LiteralResolve{
				Type:  arrow.PrimitiveTypes.Int32,
				Value: v,
			}
		}

		makeLitStr := func(v string) Expression {
			return &LiteralResolve{
				Type:  arrow.BinaryTypes.String,
				Value: v,
			}
		}

		// ----------------------------------------
		// 1. CAST Literal Int32 -> Float64
		// ----------------------------------------
		t.Run("Literal_Int32_to_Float64", func(t *testing.T) {
			ce := &CastExpr{
				Expr:       makeLitInt(42),
				TargetType: arrow.PrimitiveTypes.Float64,
			}

			arr, err := EvalExpression(ce, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			out := arr.(*array.Float64)
			for i := 0; i < out.Len(); i++ {
				if out.Value(i) != 42.0 {
					t.Fatalf("expected 42.0, got %f", out.Value(i))
				}
			}
		})

		// ----------------------------------------
		// 2. CAST Literal String -> Int32 (invalid)
		// ----------------------------------------
		t.Run("Literal_String_to_Int32_error", func(t *testing.T) {
			ce := &CastExpr{
				Expr:       makeLitStr("hello"),
				TargetType: arrow.PrimitiveTypes.Int32,
			}

			_, err := EvalCast(ce, rc)
			if err == nil {
				t.Fatalf("expected cast error, got nil")
			}
			t.Logf("Cast string->int32 error : %v\n", err)
		})

		// ----------------------------------------
		// 3. CAST Column (age int32) -> Float64
		// ----------------------------------------
		t.Run("Column_age_to_Float64", func(t *testing.T) {
			col := &ColumnResolve{Name: "age"}

			ce := &CastExpr{
				Expr:       col,
				TargetType: arrow.PrimitiveTypes.Float64,
			}

			arr, err := EvalExpression(ce, rc)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			orig := rc.Columns[rc.Schema.FieldIndices("age")[0]].(*array.Int32)
			out := arr.(*array.Float64)

			if orig.Len() != out.Len() {
				t.Fatalf("length mismatch: %d vs %d", orig.Len(), out.Len())
			}

			for i := 0; i < orig.Len(); i++ {
				if float64(orig.Value(i)) != out.Value(i) {
					t.Fatalf("at %d expected %f, got %f",
						i, float64(orig.Value(i)), out.Value(i))
				}
			}
		})

		// ----------------------------------------
		// 4. Invalid target type (like trying cast to LargeBinary)
		// ----------------------------------------
		t.Run("InvalidTargetType", func(t *testing.T) {
			ce := &CastExpr{
				Expr:       makeLitInt(5),
				TargetType: arrow.BinaryTypes.LargeBinary,
			}

			_, err := EvalExpression(ce, rc)
			if err == nil {
				t.Fatalf("expected error for invalid cast, got nil")
			}
		})

		// ----------------------------------------
		// 5. Interface + String check
		// ----------------------------------------
		ce := &CastExpr{
			Expr:       makeLitInt(1),
			TargetType: arrow.PrimitiveTypes.Float64,
		}

		// no panic = success
		ce.ExprNode()

		t.Logf("%s\n", ce.String())
	})

}

// InvariantExpr is a dummy expression that always returns an error when evaluated
type InvariantExpr struct{}

func (ie *InvariantExpr) ExprNode() {}
func (ie *InvariantExpr) String() string {
	return "InvariantExpr"
}
func TestInvariantExpr(t *testing.T) {
	t.Run("InvariantExpr", func(t *testing.T) {
		_, err := EvalExpression(&InvariantExpr{}, nil)
		if err == nil {
			t.Fatalf("expected error for invariant expr eval, got nil")
		}
	})
}

// Tests for ExprDataType and the helper type-inference functions.
func TestExprDataType(t *testing.T) {
	// simple schema used for column resolution tests
	schema := arrow.NewSchema([]arrow.Field{{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)

	t.Run("Literal_ReturnsType", func(t *testing.T) {
		lit := &LiteralResolve{Type: arrow.PrimitiveTypes.Int32}
		got, _ := ExprDataType(lit, nil)
		if got.ID() != arrow.INT32 {
			t.Fatalf("expected INT32, got %s", got)
		}
	})

	t.Run("Column_ReturnsSchemaType", func(t *testing.T) {
		col := &ColumnResolve{Name: "age"}
		got, _ := ExprDataType(col, schema)
		if got.ID() != arrow.INT32 {
			t.Fatalf("expected INT32, got %s", got)
		}
	})

	t.Run("Column_Unknown_Panics", func(t *testing.T) {
		_, err := ExprDataType(&ColumnResolve{Name: "missing"}, schema)
		if err == nil {
			t.Fatalf("expected error for unknown column, got none")
		}
	})

	t.Run("Alias_PreservesType", func(t *testing.T) {
		a := &Alias{Expr: &LiteralResolve{Type: arrow.PrimitiveTypes.Float64}, Name: "f"}
		got, _ := ExprDataType(a, schema)
		if got.ID() != arrow.FLOAT64 {
			t.Fatalf("expected FLOAT64, got %s", got)
		}
	})

	t.Run("Cast_ReturnsTargetType", func(t *testing.T) {
		c := &CastExpr{Expr: &LiteralResolve{Type: arrow.PrimitiveTypes.Int32}, TargetType: arrow.PrimitiveTypes.Float64}
		got, _ := ExprDataType(c, schema)
		if got.ID() != arrow.FLOAT64 {
			t.Fatalf("expected FLOAT64, got %s", got)
		}
	})

	t.Run("Binary_Arithmetic_PromotesToFloat64", func(t *testing.T) {
		be := &BinaryExpr{Left: &LiteralResolve{Type: arrow.PrimitiveTypes.Int32}, Op: Addition, Right: &LiteralResolve{Type: arrow.PrimitiveTypes.Int32}}
		got, _ := ExprDataType(be, schema)
		if got.ID() != arrow.FLOAT64 {
			t.Fatalf("expected FLOAT64 from numericPromotion, got %s", got)
		}
	})

	t.Run("Binary_Comparison_ReturnsBoolean", func(t *testing.T) {
		be := &BinaryExpr{Left: &LiteralResolve{Type: arrow.PrimitiveTypes.Int32}, Op: Equal, Right: &LiteralResolve{Type: arrow.PrimitiveTypes.Int32}}
		got, _ := ExprDataType(be, schema)
		if got.ID() != arrow.BOOL {
			t.Fatalf("expected BOOL from comparison, got %s", got)
		}
	})

	t.Run("ScalarFunction_Upper_String", func(t *testing.T) {
		sf := &ScalarFunction{Function: Upper, Arguments: &LiteralResolve{Type: arrow.BinaryTypes.String}}
		got, _ := ExprDataType(sf, schema)
		if got.ID() != arrow.STRING {
			t.Fatalf("expected STRING from Upper, got %s", got)
		}
	})

	t.Run("UnsupportedExpr_Panics", func(t *testing.T) {
		// InvariantExpr is a test-only expr that should hit the default case
		_, err := ExprDataType(&InvariantExpr{}, schema)
		if err == nil {
			t.Fatalf("expected error for unsupported expr type, got none")
		}
	})
}

func TestInferBinaryType(t *testing.T) {
	t.Run("Arithmetic_ReturnsNumericPromotion", func(t *testing.T) {
		got := inferBinaryType(arrow.PrimitiveTypes.Int32, Addition, arrow.PrimitiveTypes.Int32)
		if got.ID() != arrow.FLOAT64 {
			t.Fatalf("expected FLOAT64 from numericPromotion, got %s", got)
		}
	})

	t.Run("Comparison_ReturnsBoolean", func(t *testing.T) {
		got := inferBinaryType(arrow.PrimitiveTypes.Int32, Equal, arrow.PrimitiveTypes.Int32)
		if got.ID() != arrow.BOOL {
			t.Fatalf("expected BOOL for comparison, got %s", got)
		}
	})

	t.Run("Logical_ReturnsBoolean", func(t *testing.T) {
		got := inferBinaryType(arrow.FixedWidthTypes.Boolean, And, arrow.FixedWidthTypes.Boolean)
		if got.ID() != arrow.BOOL {
			t.Fatalf("expected BOOL for logical op, got %s", got)
		}
	})

}

func TestNumericPromotion(t *testing.T) {
	t.Run("Int32_Int32_ToFloat64", func(t *testing.T) {
		got := numericPromotion(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32)
		if got.ID() != arrow.FLOAT64 {
			t.Fatalf("expected FLOAT64 from numericPromotion, got %s", got)
		}
	})
}

func TestInferScalarFunctionType(t *testing.T) {
	t.Run("Upper_Lower_String", func(t *testing.T) {
		got := inferScalarFunctionType(Upper, arrow.BinaryTypes.String)
		if got.ID() != arrow.STRING {
			t.Fatalf("expected STRING for Upper/Lower, got %s", got)
		}
	})

	t.Run("Upper_NonString_Panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic for Upper on non-string, got none")
			}
		}()
		_ = inferScalarFunctionType(Upper, arrow.PrimitiveTypes.Int32)
	})

	t.Run("Abs_Round_ReturnSameType", func(t *testing.T) {
		got := inferScalarFunctionType(Abs, arrow.PrimitiveTypes.Int32)
		if got.ID() != arrow.INT32 {
			t.Fatalf("expected same input type for Abs/Round, got %s", got)
		}
	})

	t.Run("UnknownFunction_Panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic for unknown function, got none")
			}
		}()
		_ = inferScalarFunctionType(supportedFunctions(9999), arrow.PrimitiveTypes.Int32)
	})
}

// test constructor methods for expressions
func TestExprInitMethods(t *testing.T) {
	t.Run("New Alias", func(t *testing.T) {
		literal := NewLiteralResolve(arrow.BinaryTypes.String, string("the golfer"))
		a := NewAlias(literal, "nickname")
		if a == nil {
			t.Fatalf("failed to create Alias expression")
		}
	})
	t.Run("New ColumnResolve", func(t *testing.T) {
		cr := NewColumnResolve("age")
		if cr == nil {
			t.Fatalf("failed to create ColumnResolve expression")
		}
	})
	t.Run("New LiteralResolve", func(t *testing.T) {
		lit := NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(42))
		if lit == nil {
			t.Fatalf("failed to create LiteralResolve expression")
		}
	})
	t.Run("New BinaryExpr", func(t *testing.T) {
		left := NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(10))
		right := NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(5))
		be := NewBinaryExpr(left, Addition, right)
		if be == nil {
			t.Fatalf("failed to create BinaryExpr expression")
		}
	})
	t.Run("New ScalarFunc", func(t *testing.T) {
		arg := NewLiteralResolve(arrow.BinaryTypes.String, string("hello"))
		sf := NewScalarFunction(Upper, arg)
		if sf == nil {
			t.Fatalf("failed to create ScalarFunction expression")
		}
	})
	t.Run("New CastExpr", func(t *testing.T) {
		expr := NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(100))
		ce := NewCastExpr(expr, arrow.PrimitiveTypes.Float64)
		if ce == nil {
			t.Fatalf("failed to create CastExpr expression")
		}
	})
	t.Run("New Expressions", func(t *testing.T) {
		literal := NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(7))
		cr := NewColumnResolve("score")
		left := NewBinaryExpr(literal, Multiplication, cr)
		sf := NewScalarFunction(Abs, left)
		ce := NewCastExpr(sf, arrow.PrimitiveTypes.Float64)
		if ce == nil {
			t.Fatalf("failed to create complex expression")
		}
		exprs := NewExpressions(literal, cr, left, sf, ce)
		if len(exprs) != 5 {
			t.Fatalf("expected 5 expressions, got %d", len(exprs))
		}
	})
}
func TestFilterBinaryExpr(t *testing.T) {
	t.Run("age == 22", func(t *testing.T) {
		rc := generateTestColumns() //4
		literal := NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(22))
		col := NewColumnResolve("age")
		be := NewBinaryExpr(col, Equal, literal)
		arr, err := EvalExpression(be, rc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		out := arr.(*array.Boolean)
		t.Logf("out:%v\n", out)
		expected := []bool{false, false, false, true}
		if len(expected) != out.Len() {
			t.Fatalf("length mismatch: expected %d, got %d", len(expected), out.Len())
		}
		for i := 0; i < out.Len(); i++ {
			if out.Value(i) != expected[i] {
				t.Fatalf("at index %d: expected %v, got %v", i, expected[i], out.Value(i))
			}
		}
	})
	t.Run("age != 22", func(t *testing.T) {
		rc := generateTestColumns()
		literal := NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(22))
		col := NewColumnResolve("age")
		be := NewBinaryExpr(col, NotEqual, literal)

		arr, err := EvalExpression(be, rc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		out := arr.(*array.Boolean)
		expected := []bool{true, true, true, false}

		if out.Len() != len(expected) {
			t.Fatalf("length mismatch: expected %d, got %d", len(expected), out.Len())
		}
		for i := 0; i < out.Len(); i++ {
			if out.Value(i) != expected[i] {
				t.Fatalf("at index %d: expected %v, got %v", i, expected[i], out.Value(i))
			}
		}
	})
	t.Run("age < 34", func(t *testing.T) {
		rc := generateTestColumns()
		literal := NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(34))
		col := NewColumnResolve("age")
		be := NewBinaryExpr(col, LessThan, literal)

		arr, err := EvalExpression(be, rc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		out := arr.(*array.Boolean)
		expected := []bool{true, false, false, true}

		if out.Len() != len(expected) {
			t.Fatalf("length mismatch: expected %d, got %d", len(expected), out.Len())
		}
		for i := 0; i < out.Len(); i++ {
			if out.Value(i) != expected[i] {
				t.Fatalf("index %d expected %v got %v", i, expected[i], out.Value(i))
			}
		}
	})
	t.Run("age <= 34", func(t *testing.T) {
		rc := generateTestColumns()
		literal := NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(34))
		col := NewColumnResolve("age")
		be := NewBinaryExpr(col, LessThanOrEqual, literal)

		arr, err := EvalExpression(be, rc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		out := arr.(*array.Boolean)
		expected := []bool{true, true, false, true}

		if out.Len() != len(expected) {
			t.Fatalf("length mismatch: expected %d, got %d", len(expected), out.Len())
		}
		for i := 0; i < out.Len(); i++ {
			if out.Value(i) != expected[i] {
				t.Fatalf("index %d expected %v got %v", i, expected[i], out.Value(i))
			}
		}
	})
	t.Run("age > 30", func(t *testing.T) {
		rc := generateTestColumns()
		literal := NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(30))
		col := NewColumnResolve("age")
		be := NewBinaryExpr(col, GreaterThan, literal)

		arr, err := EvalExpression(be, rc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		out := arr.(*array.Boolean)
		expected := []bool{false, true, true, false}

		if out.Len() != len(expected) {
			t.Fatalf("length mismatch: expected %d, got %d", len(expected), out.Len())
		}
		for i := 0; i < out.Len(); i++ {
			if out.Value(i) != expected[i] {
				t.Fatalf("index %d expected %v got %v", i, expected[i], out.Value(i))
			}
		}
	})
	t.Run("age >= 34", func(t *testing.T) {
		rc := generateTestColumns()
		literal := NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(34))
		col := NewColumnResolve("age")
		be := NewBinaryExpr(col, GreaterThanOrEqual, literal)

		arr, err := EvalExpression(be, rc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		out := arr.(*array.Boolean)
		expected := []bool{false, true, true, false}

		if out.Len() != len(expected) {
			t.Fatalf("length mismatch: expected %d, got %d", len(expected), out.Len())
		}
		for i := 0; i < out.Len(); i++ {
			if out.Value(i) != expected[i] {
				t.Fatalf("index %d expected %v got %v", i, expected[i], out.Value(i))
			}
		}
	})
	t.Run("logical AND: (age > 30) AND is_active", func(t *testing.T) {
		rc := generateTestColumns()

		left := NewBinaryExpr(
			NewColumnResolve("age"),
			GreaterThan,
			NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(30)),
		)

		right := NewBinaryExpr(
			NewColumnResolve("is_active"),
			Equal,
			NewLiteralResolve(arrow.FixedWidthTypes.Boolean, true),
		)

		be := NewBinaryExpr(left, And, right)
		arr, err := EvalExpression(be, rc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		out := arr.(*array.Boolean)
		expected := []bool{false, false, true, false}

		for i := range expected {
			if out.Value(i) != expected[i] {
				t.Fatalf("index %d: expected %v got %v", i, expected[i], out.Value(i))
			}
		}
	})
	t.Run("logical OR: (age < 30) OR is_active", func(t *testing.T) {
		rc := generateTestColumns()

		left := NewBinaryExpr(
			NewColumnResolve("age"),
			LessThan,
			NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(30)),
		)

		right := NewBinaryExpr(
			NewColumnResolve("is_active"),
			Equal,
			NewLiteralResolve(arrow.FixedWidthTypes.Boolean, true),
		)

		be := NewBinaryExpr(left, Or, right)
		arr, err := EvalExpression(be, rc)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		out := arr.(*array.Boolean)
		expected := []bool{true, false, true, true}

		for i := range expected {
			if out.Value(i) != expected[i] {
				t.Fatalf("index %d: expected %v got %v", i, expected[i], out.Value(i))
			}
		}
	})

}

func TestFilterBinaryExpr_InvalidTypes(t *testing.T) {
	rc := generateTestColumns() // 4 rows

	// LEFT = age (int32)
	left := NewColumnResolve("age")

	// RIGHT = name (string)  → mismatched type
	right := NewColumnResolve("name")

	t.Run("invalid Equal", func(t *testing.T) {
		be := NewBinaryExpr(left, Equal, right)
		_, err := EvalExpression(be, rc)
		if err == nil {
			t.Fatalf("expected error for mismatched datatypes (Equal), got nil")
		}
	})

	t.Run("invalid NotEqual", func(t *testing.T) {
		be := NewBinaryExpr(left, NotEqual, right)
		_, err := EvalExpression(be, rc)
		if err == nil {
			t.Fatalf("expected error for mismatched datatypes (NotEqual), got nil")
		}
	})

	t.Run("invalid LessThan", func(t *testing.T) {
		be := NewBinaryExpr(left, LessThan, right)
		_, err := EvalExpression(be, rc)
		if err == nil {
			t.Fatalf("expected error for mismatched datatypes (LessThan), got nil")
		}
	})

	t.Run("invalid LessThanOrEqual", func(t *testing.T) {
		be := NewBinaryExpr(left, LessThanOrEqual, right)
		_, err := EvalExpression(be, rc)
		if err == nil {
			t.Fatalf("expected error for mismatched datatypes (LessThanOrEqual), got nil")
		}
	})

	t.Run("invalid GreaterThan", func(t *testing.T) {
		be := NewBinaryExpr(left, GreaterThan, right)
		_, err := EvalExpression(be, rc)
		if err == nil {
			t.Fatalf("expected error for mismatched datatypes (GreaterThan), got nil")
		}
	})

	t.Run("invalid GreaterThanOrEqual", func(t *testing.T) {
		be := NewBinaryExpr(left, GreaterThanOrEqual, right)
		_, err := EvalExpression(be, rc)
		if err == nil {
			t.Fatalf("expected error for mismatched datatypes (GreaterThanOrEqual), got nil")
		}
	})

	t.Run("invalid AND", func(t *testing.T) {
		be := NewBinaryExpr(left, And, right)
		_, err := EvalExpression(be, rc)
		if err == nil {
			t.Fatalf("expected error for mismatched datatypes (AND), got nil")
		}
	})

	t.Run("invalid OR", func(t *testing.T) {
		be := NewBinaryExpr(left, Or, right)
		_, err := EvalExpression(be, rc)
		if err == nil {
			t.Fatalf("expected error for mismatched datatypes (OR), got nil")
		}
	})
}

func TestCompileRegEx(t *testing.T) {
	t.Run("starts with abc", func(t *testing.T) {
		sqlString := "abc%"
		expectedRegEx := "^abc.*"
		res := compileSqlRegEx(sqlString)
		if res != expectedRegEx {
			t.Fatalf("expected %v, received %s", expectedRegEx, res)
		}
	})

	t.Run("only % (matches anything)", func(t *testing.T) {
		sqlString := "%"
		expectedRegEx := ".*"
		res := compileSqlRegEx(sqlString)
		if res != expectedRegEx {
			t.Fatalf("expected %v, received %s", expectedRegEx, res)
		}
	})

	t.Run("starts with foo", func(t *testing.T) {
		sqlString := "foo%"
		expectedRegEx := "^foo.*"
		res := compileSqlRegEx(sqlString)
		if res != expectedRegEx {
			t.Fatalf("expected %v, received %s", expectedRegEx, res)
		}
	})

	t.Run("ends with xyz", func(t *testing.T) {
		sqlString := "%xyz"
		expectedRegEx := ".*xyz$"
		res := compileSqlRegEx(sqlString)
		if res != expectedRegEx {
			t.Fatalf("expected %v, received %s", expectedRegEx, res)
		}
	})

	t.Run("contains dog", func(t *testing.T) {
		sqlString := "%dog%"
		expectedRegEx := ".*dog.*"
		res := compileSqlRegEx(sqlString)
		if res != expectedRegEx {
			t.Fatalf("expected %v, received %s", expectedRegEx, res)
		}
	})

	t.Run("exactly 3 chars", func(t *testing.T) {
		sqlString := "___"
		expectedRegEx := "^...$"
		res := compileSqlRegEx(sqlString)
		if res != expectedRegEx {
			t.Fatalf("expected %v, received %s", expectedRegEx, res)
		}
	})

	t.Run("a_z pattern", func(t *testing.T) {
		sqlString := "a_z"
		expectedRegEx := "^a.z$"
		res := compileSqlRegEx(sqlString)
		if res != expectedRegEx {
			t.Fatalf("expected %v, received %s", expectedRegEx, res)
		}
	})

	t.Run("error-__", func(t *testing.T) {
		sqlString := "error-__"
		expectedRegEx := "^error-..$"
		res := compileSqlRegEx(sqlString)
		if res != expectedRegEx {
			t.Fatalf("expected %v, received %s", expectedRegEx, res)
		}
	})

	t.Run("3 chars then log", func(t *testing.T) {
		sqlString := "___log"
		expectedRegEx := "^...log$"
		res := compileSqlRegEx(sqlString)
		if res != expectedRegEx {
			t.Fatalf("expected %v, received %s", expectedRegEx, res)
		}
	})

	t.Run("file-%.txt", func(t *testing.T) {
		sqlString := "%file-%.txt"
		expectedRegEx := ".*file-.*\\.txt$"
		res := compileSqlRegEx(sqlString)
		if res != expectedRegEx {
			t.Fatalf("expected %v, received %s", expectedRegEx, res)
		}
	})
}

func TestLikeOperatorSQL(t *testing.T) {
	t.Run("name starts with a", func(t *testing.T) {
		rc := generateTestColumns()
		sqlStatment := "A%"
		whereStatment := NewBinaryExpr(NewColumnResolve("name"), Like, NewLiteralResolve(arrow.BinaryTypes.String, string(sqlStatment)))
		boolMask, err := EvalExpression(whereStatment, rc)
		if err != nil {
			t.Fatalf("unexpected error from EvalExpression")
		}
		mask, ok := boolMask.(*array.Boolean)
		if !ok {
			t.Fatalf("expected array type to be of type boolean but got %T, error:%v", mask, err)
		}
		expectedMask := []bool{true, false, false, false}
		if mask.Len() != len(expectedMask) {
			t.Fatalf("expected boolean array len to be %d but got %d", len(expectedMask), mask.Len())
		}
		for i := 0; i < mask.Len(); i++ {
			if mask.Value(i) != expectedMask[i] {
				t.Fatalf("expected mask[%d] to be %v but got %v", i, expectedMask[i], mask.Value(i))
			}
		}
	})
	t.Run("name contains li", func(t *testing.T) {
		rc := generateTestColumns()
		sqlStatment := "%li%"
		whereStatment := NewBinaryExpr(NewColumnResolve("name"), Like, NewLiteralResolve(arrow.BinaryTypes.String, string(sqlStatment)))

		boolMask, err := EvalExpression(whereStatment, rc)
		if err != nil {
			t.Fatalf("unexpected error from EvalExpression")
		}

		mask, ok := boolMask.(*array.Boolean)
		if !ok {
			t.Fatalf("expected array type to be boolean, got %T, error:%v", mask, err)
		}

		expectedMask := []bool{true, false, true, false} // Alice, Charlie

		if mask.Len() != len(expectedMask) {
			t.Fatalf("expected mask len %d, got %d", len(expectedMask), mask.Len())
		}
		for i := 0; i < mask.Len(); i++ {
			if mask.Value(i) != expectedMask[i] {
				t.Fatalf("expected mask[%d]=%v but got %v", i, expectedMask[i], mask.Value(i))
			}
		}
	})
	t.Run("name ends with d", func(t *testing.T) {
		rc := generateTestColumns()
		sqlStatment := "%d"
		whereStatment := NewBinaryExpr(NewColumnResolve("name"), Like, NewLiteralResolve(arrow.BinaryTypes.String, string(sqlStatment)))

		boolMask, err := EvalExpression(whereStatment, rc)
		if err != nil {
			t.Fatalf("unexpected error from EvalExpression")
		}

		mask, ok := boolMask.(*array.Boolean)
		if !ok {
			t.Fatalf("expected array type boolean, got %T, error:%v", mask, err)
		}

		expectedMask := []bool{false, false, false, true} // only David ends with d

		if mask.Len() != len(expectedMask) {
			t.Fatalf("expected mask len %d, got %d", len(expectedMask), mask.Len())
		}
		for i := 0; i < mask.Len(); i++ {
			if mask.Value(i) != expectedMask[i] {
				t.Fatalf("expected mask[%d]=%v but got %v", i, expectedMask[i], mask.Value(i))
			}
		}
	})
	t.Run("name is exactly 5 letters", func(t *testing.T) {
		rc := generateTestColumns()
		sqlStatment := "_____"
		whereStatment := NewBinaryExpr(NewColumnResolve("name"), Like, NewLiteralResolve(arrow.BinaryTypes.String, string(sqlStatment)))

		boolMask, err := EvalExpression(whereStatment, rc)
		if err != nil {
			t.Fatalf("unexpected error from EvalExpression")
		}

		mask, ok := boolMask.(*array.Boolean)
		if !ok {
			t.Fatalf("expected boolean array got %T, error:%v", mask, err)
		}

		expectedMask := []bool{true, false, false, true} // Alice (5), David (5)

		if mask.Len() != len(expectedMask) {
			t.Fatalf("expected mask len %d, got %d", len(expectedMask), mask.Len())
		}
		for i := 0; i < mask.Len(); i++ {
			if mask.Value(i) != expectedMask[i] {
				t.Fatalf("expected mask[%d]=%v but got %v", i, expectedMask[i], mask.Value(i))
			}
		}
	})
	t.Run("name starts with Ch", func(t *testing.T) {
		rc := generateTestColumns()
		sqlStatment := "Ch%"
		whereStatment := NewBinaryExpr(NewColumnResolve("name"), Like, NewLiteralResolve(arrow.BinaryTypes.String, string(sqlStatment)))

		boolMask, err := EvalExpression(whereStatment, rc)
		if err != nil {
			t.Fatalf("unexpected error from EvalExpression")
		}

		mask, ok := boolMask.(*array.Boolean)
		if !ok {
			t.Fatalf("expected boolean array got %T, error:%v", mask, err)
		}

		expectedMask := []bool{false, false, true, false} // only Charlie starts with Ch

		if mask.Len() != len(expectedMask) {
			t.Fatalf("expected mask len %d, got %d", len(expectedMask), mask.Len())
		}
		for i := 0; i < mask.Len(); i++ {
			if mask.Value(i) != expectedMask[i] {
				t.Fatalf("expected mask[%d]=%v but got %v", i, expectedMask[i], mask.Value(i))
			}
		}
	})
}
