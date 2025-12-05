package filter

import (
	"errors"
	"io"
	"opti-sql-go/Expr"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

func TestFilterInit(t *testing.T) {
	proj := basicProject()
	predicate := Expr.NewBinaryExpr(Expr.NewColumnResolve("age"), Expr.GreaterThan, Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, 30))
	_, err := NewFilterExec(proj, predicate)
	if err != nil {
		t.Fatalf("failed to create filter exec: %v", err)
	}
}
func TestFilterInit_1(t *testing.T) {
	t.Run("simple greater-than predicate", func(t *testing.T) {
		proj := basicProject()
		predicate := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("age"),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, 30),
		)
		_, err := NewFilterExec(proj, predicate)
		if err != nil {
			t.Fatalf("failed to create filter exec: %v", err)
		}
	})

	t.Run("equals predicate", func(t *testing.T) {
		proj := basicProject()
		predicate := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("is_active"),
			Expr.Equal,
			Expr.NewLiteralResolve(arrow.FixedWidthTypes.Boolean, true),
		)
		_, err := NewFilterExec(proj, predicate)
		if err != nil {
			t.Fatalf("failed to create filter exec: %v", err)
		}
	})

	t.Run("invalid column name", func(t *testing.T) {
		proj := basicProject()
		predicate := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("does_not_exist"),
			Expr.Equal,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, 1),
		)
		_, err := NewFilterExec(proj, predicate)
		if err == nil {
			t.Fatalf("expected error for missing column, got nil")
		}
	})

	t.Run("nil predicate should fail", func(t *testing.T) {
		proj := basicProject()
		_, err := NewFilterExec(proj, nil)
		if err == nil {
			t.Fatalf("expected error for nil predicate")
		}
	})
}

func TestFilterExec_BasicPredicates(t *testing.T) {

	t.Run("age > 30 returns correct rows", func(t *testing.T) {
		proj := basicProject()

		pred := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("age"),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, 30),
		)

		f, _ := NewFilterExec(proj, pred)

		rb, err := f.Next(10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// --- use ColumnByName + correct type assertion ---
		raw, _ := rb.ColumnByName("age")
		ageCol, ok := raw.(*array.Int32)
		if !ok {
			t.Fatalf("expected Int32 column, got %T", raw)
		}

		expected := []int32{34, 45, 31, 40, 36, 50}

		for i := range expected {
			if ageCol.Value(i) != expected[i] {
				t.Fatalf("index %d expected %d got %d", i, expected[i], ageCol.Value(i))
			}
		}
	})

	t.Run("is_active == true", func(t *testing.T) {
		proj := basicProject()

		pred := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("is_active"),
			Expr.Equal,
			Expr.NewLiteralResolve(arrow.FixedWidthTypes.Boolean, true),
		)

		f, _ := NewFilterExec(proj, pred)

		rb, err := f.Next(20)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		raw, _ := rb.ColumnByName("is_active")
		boolCol, ok := raw.(*array.Boolean)
		if !ok {
			t.Fatalf("expected Boolean column, got %T", raw)
		}

		for i := 0; i < boolCol.Len(); i++ {
			if !boolCol.Value(i) {
				t.Fatalf("expected all rows to be true")
			}
		}
	})

	t.Run("salary < 60000", func(t *testing.T) {
		proj := basicProject()

		pred := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("salary"),
			Expr.LessThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Float64, float64(60000.0)),
		)

		f, _ := NewFilterExec(proj, pred)

		rb, err := f.Next(10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		raw, _ := rb.ColumnByName("salary")
		salCol, ok := raw.(*array.Float64)
		if !ok {
			t.Fatalf("expected Float64 column, got %T", raw)
		}

		expected := []float64{54000.0, 45000.0}

		for i := range expected {
			if salCol.Value(i) != expected[i] {
				t.Fatalf("expected %v got %v", expected[i], salCol.Value(i))
			}
		}
	})

	t.Run("department == 'Engineering'", func(t *testing.T) {
		proj := basicProject()

		pred := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("department"),
			Expr.Equal,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, "Engineering"),
		)

		f, _ := NewFilterExec(proj, pred)

		rb, err := f.Next(10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		raw, _ := rb.ColumnByName("department")
		deptCol, ok := raw.(*array.String)
		if !ok {
			t.Fatalf("expected String column, got %T", raw)
		}

		for i := 0; i < deptCol.Len(); i++ {
			if deptCol.Value(i) != "Engineering" {
				t.Fatalf("expected Engineering got %s", deptCol.Value(i))
			}
		}
	})
}

func TestFilterExec_EdgeCases(t *testing.T) {

	t.Run("Next(0) returns empty batch", func(t *testing.T) {
		proj := basicProject()

		pred := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("age"),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, 20),
		)

		f, _ := NewFilterExec(proj, pred)

		_, err := f.Next(0)
		if err == nil {
			t.Fatalf("expected error but got %v", err)
		}
	})

	t.Run("EOF after consuming all rows", func(t *testing.T) {
		proj := basicProject()

		pred := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("age"),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, 0),
		)

		f, _ := NewFilterExec(proj, pred)

		_, _ = f.Next(50)
		_, err := f.Next(10)

		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF, got %v", err)
		}
	})

	t.Run("predicate that always returns false", func(t *testing.T) {
		proj := basicProject()

		pred := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("age"),
			Expr.Equal,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, -1),
		)

		f, _ := NewFilterExec(proj, pred)

		_, err := f.Next(20)
		if err == nil {
			t.Fatalf("expected EOF error but got nil")
		}
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF error but got %v", err)
		}
	})

	t.Run("incompatible predicate types → error", func(t *testing.T) {
		proj := basicProject()

		pred := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("age"), // int32
			Expr.Equal,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, "bad"), // string
		)

		_, err := NewFilterExec(proj, pred)
		if err == nil {
			t.Fatalf("expected type error for invalid predicate")
		}
	})
}

func TestFilterExecVariantCase(t *testing.T) {
	t.Run("filter done", func(t *testing.T) {
		proj := basicProject()
		predicate := Expr.NewBinaryExpr(Expr.NewColumnResolve("age"), Expr.GreaterThan, Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, 30))
		f, _ := NewFilterExec(proj, predicate)
		_, err := f.Next(1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		f.done = true
		_, err = f.Next(1)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF error, got %v", err)
		}

	})
	t.Run("filter schema ", func(t *testing.T) {
		proj := basicProject()
		predicate := Expr.NewBinaryExpr(Expr.NewColumnResolve("age"), Expr.GreaterThan, Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, 30))
		f, _ := NewFilterExec(proj, predicate)
		t.Logf("%s", f.Schema())
		if !f.schema.Equal(proj.Schema()) {
			t.Fatalf("expected schema to match input schema")
		}

	})
	t.Run("filter close ", func(t *testing.T) {
		proj := basicProject()
		predicate := Expr.NewBinaryExpr(Expr.NewColumnResolve("age"), Expr.GreaterThan, Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, 30))
		f, _ := NewFilterExec(proj, predicate)
		if f.Close() != nil {
			t.Fatalf("expected nil error on close")
		}
	})
	t.Run("filter unsupported binary operator ", func(t *testing.T) {
		proj := basicProject()
		predicate := Expr.NewBinaryExpr(Expr.NewColumnResolve("age"), Expr.Addition, Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, 30))
		_, err := NewFilterExec(proj, predicate)
		if err == nil {
			t.Fatalf("expected error for unsupported binary operator")
		}
	})
	t.Run("filter empty column resolve ", func(t *testing.T) {
		proj := basicProject()
		predicate := Expr.NewColumnResolve("doesnt-exist")
		_, err := NewFilterExec(proj, predicate)
		if err == nil {
			t.Fatalf("expected error for empty column resolve")
		}

	})

}

func TestFilterBuffer(t *testing.T) {
	t.Run("test", func(t *testing.T) {

		proj := basicProject()
		predicate := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("age"),
			Expr.GreaterThan,
			Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int32, 30),
		)
		f, err := NewFilterExec(proj, predicate)
		if err != nil {
			t.Fatalf("failed to create filter exec: %v", err)
		}
		_, err = f.Next(5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		_, err = f.Next(5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

	})
}
