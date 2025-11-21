package project

import (
	"math"
	"opti-sql-go/Expr"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

func generateData() ([]string, []any) {
	names := []string{"id", "name", "age", "active", "score"}
	values := []any{
		[]int{1, 2, 3, 4, 5, 6},
		[]string{"Ainsley Coffey", "Kody Frazier", "Octavia Truong", "Ayan Gonzalez", "Abigail Castro", "Clay McDaniel"},
		[]int8{10, 12, 35, 76, 42, 63},
		[]bool{false, true, false, true, true, true},
		[]float32{98.6, 75.4, 88.1, 92.3, 79.5, 85.0},
	}
	return names, values

}

/*
project: column
project: column1, column2, column3
sql : select id,age,name from table
*/
func TestProjectExec_Column_sql(t *testing.T) {
	names, cols := generateData()
	t.Run("select id", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)
		exprs := []Expr.Expression{
			&Expr.ColumnResolve{Name: "id"},
		}
		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(6)
		if err != nil {
			t.Fatalf("err:%v\n", err)
		}
		t.Logf("rc:%v\n", rc)
		column := rc.Columns[0]
		if column.DataType() != arrow.PrimitiveTypes.Int64 {
			t.Fatalf("expected int64, got %s", column.DataType().Name())
		}
		idCol, ok := column.(*array.Int64)
		if !ok {
			t.Fatalf("expected Int64 array, got %T", column)
		}
		expectedValues := []int64{1, 2, 3, 4, 5, 6}
		for i, v := range expectedValues {
			if idCol.Value(i) != v {
				t.Fatalf("at index %d: expected %d, got %d", i, v, idCol.Value(i))
			}
		}

	})
	t.Run("select name", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)
		exprs := []Expr.Expression{
			&Expr.ColumnResolve{Name: "name"},
		}
		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(6)
		if err != nil {
			t.Fatalf("err:%v\n", err)
		}
		t.Logf("rc:%v\n", rc)
		column := rc.Columns[0]
		if column.DataType() != arrow.BinaryTypes.String {
			t.Fatalf("expected string, got %s", column.DataType().Name())
		}
		nameCol, ok := column.(*array.String)
		if !ok {
			t.Fatalf("expected String array, got %T", column)
		}
		expectedValues := []string{"Ainsley Coffey", "Kody Frazier", "Octavia Truong", "Ayan Gonzalez", "Abigail Castro", "Clay McDaniel"}
		for i, v := range expectedValues {
			if nameCol.Value(i) != v {
				t.Fatalf("at index %d: expected %s, got %s", i, v, nameCol.Value(i))
			}
		}
	})
	t.Run("select age", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)
		exprs := []Expr.Expression{
			&Expr.ColumnResolve{Name: "age"},
		}
		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(6)
		if err != nil {
			t.Fatalf("err:%v\n", err)
		}
		t.Logf("rc:%v\n", rc)
		column := rc.Columns[0]
		if column.DataType() != arrow.PrimitiveTypes.Int8 {
			t.Fatalf("expected int8, got %s", column.DataType().Name())
		}
		ageCol, ok := column.(*array.Int8)
		if !ok {
			t.Fatalf("expected Int8 array, got %T", column)
		}
		expectedValues := []int8{10, 12, 35, 76, 42, 63}
		for i, v := range expectedValues {
			if ageCol.Value(i) != v {
				t.Fatalf("at index %d: expected %d, got %d", i, v, ageCol.Value(i))
			}
		}
	})

}

// these test with no base table
// select 1 from table
func TestProjectExec_Literal_sql(t *testing.T) {
	names, cols := generateData()
	t.Run("select 1", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)
		exprs := []Expr.Expression{
			&Expr.LiteralResolve{Type: arrow.PrimitiveTypes.Int64, Value: int64(4)},
		}
		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(1)
		if err != nil {
			t.Fatalf("err:%v\n", err)
		}
		column := rc.Columns[0]
		if column.DataType() != arrow.PrimitiveTypes.Int64 {
			t.Fatalf("expected int64, got %s", column.DataType().Name())
		}
		if column.ValueStr(0) != "4" {
			t.Fatalf("expected 4, got %s", column.ValueStr(0))
		}

	})
	t.Run("select 'hello'", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)
		exprs := []Expr.Expression{
			&Expr.LiteralResolve{Type: arrow.BinaryTypes.String, Value: string("hello")},
		}
		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(1)
		if err != nil {
			t.Fatalf("err:%v\n", err)
		}
		column := rc.Columns[0]
		if column.DataType() != arrow.BinaryTypes.String {
			t.Fatalf("expected column types to be of type string but received %s\n", column.DataType())
		}
		if column.ValueStr(0) != "hello" {
			t.Fatalf("expected hello, got %s", column.ValueStr(0))
		}
		t.Logf("rc:%v\n", rc)

	})
	t.Run("select 3.14", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)
		exprs := []Expr.Expression{
			&Expr.LiteralResolve{Type: arrow.PrimitiveTypes.Float32, Value: float32(3.14)},
		}
		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(2)
		if err != nil {
			t.Fatalf("err:%v\n", err)
		}
		column := rc.Columns[0]
		if column.DataType() != arrow.PrimitiveTypes.Float32 {
			t.Fatalf("expected column types to be of type string but received %s\n", column.DataType())
		}
		floatArr, ok := column.(*array.Float32)
		if !ok {
			t.Fatalf("expected Float32 array, got %T", column)
		}
		for i := 0; i < floatArr.Len(); i++ {
			if floatArr.Value(i) != float32(3.14) {
				t.Fatalf("at index %d: expected %f, got %f", i, float32(3.14), floatArr.Value(i))
			}
		}
		t.Logf("rc:%v\n", rc)

	})

}

/*
Project: Literal |Operator| Literal
*/
func TestProjectExec_Literal_Literal(t *testing.T) {
	names, cols := generateData()
	t.Run("Age plus constant", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)
		exprs := []Expr.Expression{
			&Expr.BinaryExpr{Left: &Expr.ColumnResolve{Name: "age"}, Op: Expr.Addition, Right: &Expr.LiteralResolve{Type: arrow.PrimitiveTypes.Int64, Value: int64(10)}},
		}
		for _, e := range exprs {
			t.Log(e.String())
		}
		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(4)
		if err != nil {
			t.Fatalf("unexpected error: %v\n", err)
		}
		ageCol, ok := rc.Columns[0].(*array.Int64)
		if !ok {
			t.Fatalf("expected column to be of type Int64 but got %T\n", rc.Columns[0])
		}
		expected := []int64{20, 22, 45, 86}
		if ageCol.Len() != len(expected) {
			t.Fatalf("mismatch in expected column length, received column of len %d", ageCol.Len())
		}
		for i := 0; i < len(expected); i++ {
			if ageCol.Value(i) != expected[i] {
				t.Fatalf("expected %d at position %d, but received %d", expected[i], i, ageCol.Value(i))
			}
		}
	})

	t.Run("id multiplied by constant", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		exprs := []Expr.Expression{
			&Expr.BinaryExpr{
				Left:  &Expr.ColumnResolve{Name: "id"},
				Op:    Expr.Multiplication,
				Right: &Expr.LiteralResolve{Type: arrow.PrimitiveTypes.Int64, Value: int64(3)},
			},
		}

		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(4)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		idCol, ok := rc.Columns[0].(*array.Int64)
		if !ok {
			t.Fatalf("expected Int64 column, got %T", rc.Columns[0])
		}

		expected := []int64{3, 6, 9, 12}

		if idCol.Len() != len(expected) {
			t.Fatalf("expected %d rows, got %d", len(expected), idCol.Len())
		}

		for i := 0; i < len(expected); i++ {
			if idCol.Value(i) != expected[i] {
				t.Fatalf("at index %d: expected %d, got %d", i, expected[i], idCol.Value(i))
			}
		}
	})
	// column |operator| nestedLiteralExpr
	t.Run("select score - (5+4)", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		nested := &Expr.BinaryExpr{
			Left:  &Expr.LiteralResolve{Type: arrow.PrimitiveTypes.Int64, Value: int64(5)},
			Op:    Expr.Addition,
			Right: &Expr.LiteralResolve{Type: arrow.PrimitiveTypes.Int64, Value: int64(4)},
		}

		exprs := []Expr.Expression{
			&Expr.BinaryExpr{
				Left:  &Expr.ColumnResolve{Name: "score"},
				Op:    Expr.Subtraction,
				Right: nested,
			},
		}

		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(6)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		scoreCol, ok := rc.Columns[0].(*array.Float32)
		if !ok {
			t.Fatalf("expected Float32 column, got %T", rc.Columns[0])
		}

		expected := []float32{
			98.6 - 9,
			75.4 - 9,
			88.1 - 9,
			92.3 - 9,
			79.5 - 9,
			85.0 - 9,
		}

		if scoreCol.Len() != len(expected) {
			t.Fatalf("expected %d rows, got %d", len(expected), scoreCol.Len())
		}

		for i := 0; i < len(expected); i++ {
			if diff := scoreCol.Value(i) - expected[i]; diff > 1e-5 || diff < -1e-5 {
				t.Fatalf("expected %f at index %d, got %f", expected[i], i, scoreCol.Value(i))
			}
		}

	})
	t.Run("select age / (2*3)", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		mult := &Expr.BinaryExpr{
			Left:  &Expr.LiteralResolve{Type: arrow.PrimitiveTypes.Int64, Value: int64(2)},
			Op:    Expr.Multiplication,
			Right: &Expr.LiteralResolve{Type: arrow.PrimitiveTypes.Int64, Value: int64(3)},
		}

		exprs := []Expr.Expression{
			&Expr.BinaryExpr{
				Left:  &Expr.ColumnResolve{Name: "age"},
				Op:    Expr.Division,
				Right: mult,
			},
		}

		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(4)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ageCol, ok := rc.Columns[0].(*array.Int64)
		if !ok {
			t.Fatalf("expected Int64 column, got %T", rc.Columns[0])
		}

		// doesnt cast by default
		// age / 6
		expected := []int64{10 / 6, 12 / 6, 35 / 6, 76 / 6}

		if ageCol.Len() != len(expected) {
			t.Fatalf("expected %d rows, got %d", len(expected), ageCol.Len())
		}

		for i := 0; i < len(expected); i++ {
			if ageCol.Value(i) != expected[i] {
				t.Fatalf("expected %d at index %d, got %d", expected[i], i, ageCol.Value(i))
			}
		}
	})
}

/*
Project: cast literal
project: cast column
*/
func TestProjectExec_CastLiteral_Column(t *testing.T) {
	names, cols := generateData()
	t.Run("select age as float32", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		exprs := []Expr.Expression{
			Expr.NewCastExpr(Expr.NewColumnResolve("age"), arrow.PrimitiveTypes.Float32),
		}

		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(6)
		if err != nil {
			t.Fatalf("error: %v", err)
		}

		col, ok := rc.Columns[0].(*array.Float32)
		if !ok {
			t.Fatalf("expected Float32 column, got %T", rc.Columns[0])
		}

		expected := []float32{10, 12, 35, 76, 42, 63}
		for i := 0; i < len(expected); i++ {
			if col.Value(i) != expected[i] {
				t.Fatalf("expected %f at %d, got %f", expected[i], i, col.Value(i))
			}
		}
	})
	t.Run("select age as int16", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		exprs := []Expr.Expression{
			Expr.NewCastExpr(Expr.NewColumnResolve("age"), arrow.PrimitiveTypes.Int16),
		}

		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(6)
		if err != nil {
			t.Fatalf("error: %v", err)
		}

		col, ok := rc.Columns[0].(*array.Int16)
		if !ok {
			t.Fatalf("expected Int16 column, got %T", rc.Columns[0])
		}

		expected := []int16{10, 12, 35, 76, 42, 63}
		for i := 0; i < len(expected); i++ {
			if col.Value(i) != expected[i] {
				t.Fatalf("expected %d at %d, got %d", expected[i], i, col.Value(i))
			}
		}
	})
	// should fail
	t.Run("select name as int32", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		exprs := []Expr.Expression{
			Expr.NewCastExpr(Expr.NewColumnResolve("name"), arrow.PrimitiveTypes.Int32),
		}

		proj, _ := NewProjectExec(memSrc, exprs)
		_, err := proj.Next(6)

		if err == nil {
			t.Fatalf("expected cast error but got nil")
		}
	})
	t.Run("select 4 as float64", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		exprs := []Expr.Expression{
			Expr.NewCastExpr(
				Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int64, int64(4)),
				arrow.PrimitiveTypes.Float64,
			),
		}

		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(6)
		if err != nil {
			t.Fatalf("error: %v", err)
		}

		col, ok := rc.Columns[0].(*array.Float64)
		if !ok {
			t.Fatalf("expected Float64 column, got %T", rc.Columns[0])
		}

		for i := 0; i < col.Len(); i++ {
			if col.Value(i) != 4.0 {
				t.Fatalf("expected 4.0 at %d, got %f", i, col.Value(i))
			}
		}

	})
	// should be a no op
	t.Run("select 'richard' as string", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		exprs := []Expr.Expression{
			Expr.NewCastExpr(
				&Expr.LiteralResolve{Type: arrow.BinaryTypes.String, Value: "richard"},
				arrow.BinaryTypes.String,
			),
		}

		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(3)
		if err != nil {
			t.Fatalf("error: %v", err)
		}

		col, ok := rc.Columns[0].(*array.String)
		if !ok {
			t.Fatalf("expected String column, got %T", rc.Columns[0])
		}

		for i := 0; i < col.Len(); i++ {
			if col.Value(i) != "richard" {
				t.Fatalf("expected 'richard' at %d, got %s", i, col.Value(i))
			}
		}
	})
}

/*
Column Name | (Operator) | type(value)
Value is applied to every element of column
*/
func TestProjectExec_Column_Literal(t *testing.T) {
	names, cols := generateData()
	t.Run("age + 10", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		exprs := Expr.NewExpressions(
			Expr.NewBinaryExpr(
				Expr.NewColumnResolve("age"),
				Expr.Addition,
				Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int8, int8(10)),
			),
		)

		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(6)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}

		out := rc.Columns[0].(*array.Int8)
		expected := []int8{20, 22, 45, 86, 52, 73}
		for i := 0; i < len(expected); i++ {
			if out.Value(i) != expected[i] {
				t.Fatalf("expected %d got %d at %d", expected[i], out.Value(i), i)
			}
		}
	})
	t.Run("score - 5.0", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		exprs := Expr.NewExpressions(
			Expr.NewBinaryExpr(
				Expr.NewColumnResolve("score"),
				Expr.Subtraction,
				Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float32, float32(5.0)),
			),
		)

		proj, _ := NewProjectExec(memSrc, exprs)
		rc, err := proj.Next(6)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		out := rc.Columns[0].(*array.Float32)
		expected := []float32{93.6, 70.4, 83.1, 87.3, 74.5, 80.0}
		for i := range expected {
			if math.Abs(float64(out.Value(i)-expected[i])) > 0.0001 {
				t.Fatalf("expected %f got %f", expected[i], out.Value(i))
			}
		}
	})

	t.Run("id * 2", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)
		exprs := Expr.NewExpressions(
			Expr.NewBinaryExpr(
				Expr.NewColumnResolve("id"),
				Expr.Multiplication,
				Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int64, int64(2)),
			),
		)

		proj, _ := NewProjectExec(memSrc, exprs)
		rc, _ := proj.Next(6)
		out := rc.Columns[0].(*array.Int64)

		expected := []int64{2, 4, 6, 8, 10, 12}
		for i := range expected {
			if out.Value(i) != expected[i] {
				t.Fatalf("expected %d got %d", expected[i], out.Value(i))
			}
		}
	})

	t.Run("score / 2", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)
		exprs := Expr.NewExpressions(
			Expr.NewBinaryExpr(
				Expr.NewColumnResolve("score"),
				Expr.Division,
				Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float32, float32(2)),
			),
		)

		proj, _ := NewProjectExec(memSrc, exprs)
		rc, _ := proj.Next(6)
		out := rc.Columns[0].(*array.Float32)

		expected := []float32{49.3, 37.7, 44.05, 46.15, 39.75, 42.5}
		for i := range expected {
			if math.Abs(float64(out.Value(i)-expected[i])) > 0.0001 {
				t.Fatalf("expected %f got %f", expected[i], out.Value(i))
			}
		}
	})
}

/*
Alias(column |operator| literal)
*/
func TestProjectExec_AliasExpr(t *testing.T) {
	names, cols := generateData()

	t.Run("alias column id → identifier", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		exprs := Expr.NewExpressions(
			Expr.NewAlias(
				Expr.NewColumnResolve("id"),
				"identifier",
			),
		)

		proj, _ := NewProjectExec(memSrc, exprs)
		schema := proj.Schema()

		if schema.Field(0).Name != "identifier" {
			t.Fatalf("expected alias name identifier, got %s", schema.Field(0).Name)
		}
	})

	t.Run("alias expression (age + 10) → boosted_age", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		exprs := Expr.NewExpressions(
			Expr.NewAlias(
				Expr.NewBinaryExpr(
					Expr.NewColumnResolve("age"),
					Expr.Addition,
					Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int8, int8(10)),
				),
				"boosted_age",
			),
		)

		proj, _ := NewProjectExec(memSrc, exprs)
		rc, _ := proj.Next(3)

		if proj.Schema().Field(0).Name != "boosted_age" {
			t.Fatalf("alias not applied")
		}

		out := rc.Columns[0].(*array.Int8)
		expected := []int8{20, 22, 45}

		for i := range expected {
			if out.Value(i) != expected[i] {
				t.Fatalf("expected %d got %d", expected[i], out.Value(i))
			}
		}
	})

	t.Run("alias literal → constant_value", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		exprs := Expr.NewExpressions(
			Expr.NewAlias(
				Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(7)),
				"constant_value",
			),
		)

		proj, _ := NewProjectExec(memSrc, exprs)
		rc, _ := proj.Next(3)

		if proj.Schema().Field(0).Name != "constant_value" {
			t.Fatalf("alias not applied")
		}

		out := rc.Columns[0].(*array.Int32)
		for i := 0; i < out.Len(); i++ {
			if out.Value(i) != 7 {
				t.Fatalf("expected literal 7, got %d", out.Value(i))
			}
		}
	})

	t.Run("alias nested expr (score - (2+3)) → final_score", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		inner := Expr.NewBinaryExpr(
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(2)),
			Expr.Addition,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, int32(3)),
		)

		exprs := Expr.NewExpressions(
			Expr.NewAlias(
				Expr.NewBinaryExpr(
					Expr.NewColumnResolve("score"),
					Expr.Subtraction,
					inner,
				),
				"final_score",
			),
		)

		proj, _ := NewProjectExec(memSrc, exprs)
		rc, _ := proj.Next(3)

		if proj.Schema().Field(0).Name != "final_score" {
			t.Fatalf("alias not applied")
		}

		out := rc.Columns[0].(*array.Float32)
		expected := []float32{
			98.6 - 5,
			75.4 - 5,
			88.1 - 5,
		}

		for i := range expected {
			if math.Abs(float64(out.Value(i)-expected[i])) > 0.0001 {
				t.Fatalf("expected %f got %f", expected[i], out.Value(i))
			}
		}
	})
}

/*
function(column/literal)
function(column |operator| literal)
function(column/literal |operator| literal/column)
*/
func TestProjectExec_FunctionExpr(t *testing.T) {
	names, cols := generateData() // id, name, age, active, score

	// ------------------------------------------------------------
	// 1. UPPER(column)
	// ------------------------------------------------------------
	t.Run("UPPER(name)", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		exprs := Expr.NewExpressions(
			Expr.NewScalarFunction(
				Expr.Upper,
				Expr.NewColumnResolve("name"),
			),
		)

		proj, _ := NewProjectExec(memSrc, exprs)
		rb, err := proj.Next(3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		out := rb.Columns[0].(*array.String)
		expected := []string{
			"AINSLEY COFFEY",
			"KODY FRAZIER",
			"OCTAVIA TRUONG",
		}

		for i := range expected {
			if out.Value(i) != expected[i] {
				t.Fatalf("expected %s got %s", expected[i], out.Value(i))
			}
		}
	})

	t.Run("LOWER('MonKey_x')", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		expr := Expr.NewLiteralResolve(arrow.BinaryTypes.String, string("MoNKey_X"))

		exprs := Expr.NewExpressions(
			Expr.NewScalarFunction(
				Expr.Lower,
				expr,
			),
		)

		proj, _ := NewProjectExec(memSrc, exprs)
		rb, _ := proj.Next(2)

		out := rb.Columns[0].(*array.String)
		t.Logf("columns: %v\n", out)
		expected := []string{
			strings.ToLower("monkey_x"),
			strings.ToLower("monkey_x"),
		}

		for i := range expected {
			if out.Value(i) != expected[i] {
				t.Fatalf("expected %s got %s", expected[i], out.Value(i))
			}
		}
	})

	// ------------------------------------------------------------
	// 3. ABS(column |operator| literal)
	// ABS(score - 100.0)
	// ------------------------------------------------------------
	t.Run("ABS(score - 100)", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		expr := Expr.NewScalarFunction(
			Expr.Abs,
			Expr.NewBinaryExpr(
				Expr.NewColumnResolve("score"),
				Expr.Subtraction,
				Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float32, float32(100)),
			),
		)

		proj, _ := NewProjectExec(memSrc, Expr.NewExpressions(expr))
		rb, _ := proj.Next(3)

		out := rb.Columns[0].(*array.Float32)

		expected := []float32{
			float32(math.Abs(98.6 - 100)),
			float32(math.Abs(75.4 - 100)),
			float32(math.Abs(88.1 - 100)),
		}

		for i := range expected {
			if math.Abs(float64(out.Value(i)-expected[i])) > 0.0001 {
				t.Fatalf("expected %f got %f", expected[i], out.Value(i))
			}
		}
	})

	// ------------------------------------------------------------
	// 4. ROUND(literal |operator| column)
	// ROUND(2.5 * score)
	// ------------------------------------------------------------
	t.Run("ROUND(2.5 * score)", func(t *testing.T) {
		memSrc, _ := NewInMemoryProjectExec(names, cols)

		expr := Expr.NewScalarFunction(
			Expr.Round,
			Expr.NewBinaryExpr(
				Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, float64(2.5)),
				Expr.Multiplication,
				Expr.NewColumnResolve("score"),
			),
		)

		proj, _ := NewProjectExec(memSrc, Expr.NewExpressions(expr))
		rb, _ := proj.Next(3)

		out := rb.Columns[0].(*array.Float64)
		expected := []float64{
			math.Round(2.5 * 98.6),
			math.Round(2.5 * 75.4),
			math.Round(2.5 * 88.1),
		}

		for i := range expected {
			if math.Abs(out.Value(i)-expected[i]) > 1 {
				t.Fatalf("expected %f got %f", expected[i], out.Value(i))
			}
		}
	})
}

/*
complex expr
ex: alias(function(column |operator| literal) |operator| literal)
TODO: not the most important thing right now since we know basic expression are fine
*/
func TestProjectExec_ComplexExpr(t *testing.T) {}
