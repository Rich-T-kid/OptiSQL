package filter

import (
	"errors"
	"io"
	"opti-sql-go/Expr"
	"opti-sql-go/operators/project"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

func generateTestColumns() ([]string, []any) {
	names := []string{
		"id",
		"name",
		"age",
		"salary",
		"is_active",
		"department",
		"rating",
		"years_experience",
	}

	columns := []any{
		[]int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		[]string{
			"Alice", "Bob", "Charlie", "David", "Eve",
			"Frank", "Grace", "Hannah", "Ivy", "Jake",
		},
		[]int32{28, 34, 45, 22, 31, 29, 40, 36, 50, 26},
		[]float64{
			70000.0, 82000.5, 54000.0, 91000.0, 60000.0,
			75000.0, 66000.0, 88000.0, 45000.0, 99000.0,
		},
		[]bool{true, false, true, true, false, false, true, true, false, true},
		[]string{
			"Engineering", "HR", "Engineering", "Sales", "Finance",
			"Sales", "Support", "Engineering", "HR", "Finance",
		},
		[]float32{4.5, 3.8, 4.2, 2.9, 5.0, 4.3, 3.7, 4.9, 4.1, 3.5},
		[]int32{1, 5, 10, 2, 7, 3, 6, 12, 4, 8},
	}

	return names, columns
}
func basicProject() *project.InMemorySource {
	names, col := generateTestColumns()
	v, _ := project.NewInMemoryProjectExec(names, col)
	return v
}
func maskAny(t *testing.T, src *project.InMemorySource, expr Expr.Expression, expected []bool) {
	t.Helper()

	// 1. Pull the record batch from the project source
	batch, err := src.Next(10)
	if err != nil {
		t.Fatalf("failed to fetch record batch: %v", err)
	}
	if batch == nil {
		t.Fatalf("expected non-nil record batch from project source")
	}

	// 2. Evaluate expression against the batch
	out, err := Expr.EvalExpression(expr, batch)
	if err != nil {
		t.Fatalf("EvalExpression error: %v", err)
	}

	// 3. Extract boolean mask
	mask, ok := out.(*array.Boolean)
	if !ok {
		t.Fatalf("expected output to be *array.Boolean, got %T", out)
	}

	// 4. Validate length matches
	if mask.Len() != len(expected) {
		t.Fatalf("expected mask length %d, got %d", len(expected), mask.Len())
	}

	// 5. Validate each element
	for i := 0; i < mask.Len(); i++ {
		if mask.Value(i) != expected[i] {
			t.Fatalf("mask[%d]: expected %v, got %v", i, expected[i], mask.Value(i))
		}
	}
}

func TestLimitInit(t *testing.T) {
	// Simple passing test
	trialProject := basicProject()
	_, err := NewLimitExec(trialProject, 4)
	if err != nil {
		t.Fatalf("error creating LimitExec :%v", err)
	}
}

func TestLimitExec_InitAndSchema(t *testing.T) {
	t.Run("Init OK", func(t *testing.T) {
		proj := basicProject()
		lim, err := NewLimitExec(proj, 5)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if lim.Schema() == nil {
			t.Fatalf("expected non-nil schema")
		}
	})

	t.Run("Init Zero Limit", func(t *testing.T) {
		proj := basicProject()
		lim, err := NewLimitExec(proj, 0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = lim.Next(3)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF for zero limit, got %v", err)
		}
	})

	t.Run("Init DoesNotModifyUnderlyingSchema", func(t *testing.T) {
		proj := basicProject()
		origSchema := proj.Schema()
		lim, _ := NewLimitExec(proj, 10)

		if !lim.Schema().Equal(origSchema) {
			t.Fatalf("schema mismatch: expected %v got %v", origSchema, lim.Schema())
		}
	})
}

func TestLimitExec_NextBehavior(t *testing.T) {
	names, cols := generateTestColumns()
	memSrc, err := project.NewInMemoryProjectExec(names, cols)
	if err != nil {
		t.Fatalf("failed to init memory source: %v", err)
	}

	t.Run("n < remaining", func(t *testing.T) {
		lim, _ := NewLimitExec(memSrc, 5)
		rb, err := lim.Next(3)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rb.RowCount != 3 {
			t.Fatalf("expected 3 rows, got %d", rb.RowCount)
		}
	})

	t.Run("n == remaining", func(t *testing.T) {
		memSrc2, _ := project.NewInMemoryProjectExec(names, cols)
		lim, _ := NewLimitExec(memSrc2, 4)
		rb, err := lim.Next(4)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rb.RowCount != 4 {
			t.Fatalf("expected 4 rows, got %d", rb.RowCount)
		}
		_, err = lim.Next(2)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF after exact match, got %v", err)
		}
	})

	t.Run("n > remaining", func(t *testing.T) {
		memSrc3, _ := project.NewInMemoryProjectExec(names, cols)
		lim, _ := NewLimitExec(memSrc3, 3)
		rb, err := lim.Next(10)
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}
		if rb.RowCount != 3 {
			t.Fatalf("expected 3 rows, got %d", rb.RowCount)
		}
		_, err = lim.Next(10)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("was expecting io.EOF but received %v", err)
		}
	})
}
func TestLimitExec_IterationUntilEOF(t *testing.T) {
	names, cols := generateTestColumns()
	memSrc, _ := project.NewInMemoryProjectExec(names, cols)

	t.Run("ConsumeInMultipleBatches", func(t *testing.T) {
		lim, _ := NewLimitExec(memSrc, 7)

		total := 0
		for {
			rb, err := lim.Next(3)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatalf("unexpected error: %v", err)
			}

			total += int(rb.RowCount)

			for _, c := range rb.Columns {
				c.Release()
			}
		}
		if total != 7 {
			t.Fatalf("expected 7 total rows, got %d", total)
		}
		if err := lim.Close(); err != nil {
			t.Fatalf("unexpected error on close: %v", err)
		}
	})

	t.Run("RequestZeroDoesNotChangeLimit", func(t *testing.T) {
		memSrc2, _ := project.NewInMemoryProjectExec(names, cols)
		lim, _ := NewLimitExec(memSrc2, 5)

		rb, err := lim.Next(0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rb.RowCount != 0 {
			t.Fatalf("expected zero rowcount, got %d", rb.RowCount)
		}

		rb2, err := lim.Next(2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rb2.RowCount != 2 {
			t.Fatalf("expected 2 rows, got %d", rb2.RowCount)
		}
		if err := lim.Close(); err != nil {
			t.Fatalf("unexpected error on close: %v", err)
		}
	})

	t.Run("AfterEOFAlwaysEOF", func(t *testing.T) {
		memSrc3, _ := project.NewInMemoryProjectExec(names, cols)
		lim, _ := NewLimitExec(memSrc3, 2)

		_, _ = lim.Next(3) // exhaust

		_, err := lim.Next(1)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF, got %v", err)
		}
		_ = lim.Close()
	})
}

/*
==============================================
//            Wild Card Test
==============================================
*/
func TestLikePercentWildcards(t *testing.T) {

	t.Run("name starts with A (A%)", func(t *testing.T) {
		src := basicProject()
		sql := "A%"

		expected := []bool{
			true, false, false, false, false,
			false, false, false, false, false,
		}

		expr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("name"),
			Expr.Like,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, sql),
		)

		maskAny(t, src, expr, expected)
	})

	t.Run("name ends with e (%e)", func(t *testing.T) {
		src := basicProject()
		sql := "%e"

		expected := []bool{
			true, // Alice
			false,
			true, // Charlie
			false,
			true, // Eve
			false,
			true, // Grace
			false,
			false,
			true, // Jake
		}

		expr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("name"),
			Expr.Like,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, sql),
		)

		maskAny(t, src, expr, expected)
	})

	t.Run("name contains 'an' (%an%)", func(t *testing.T) {
		src := basicProject()
		sql := "%an%"

		expected := []bool{
			false, false, false, false, false,
			true, // Frank
			false,
			true, // Hannah
			false,
			false,
		}

		expr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("name"),
			Expr.Like,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, sql),
		)

		maskAny(t, src, expr, expected)
	})

	t.Run("wildcard only (%) matches all rows", func(t *testing.T) {
		src := basicProject()
		sql := "%"

		expected := []bool{
			true, true, true, true, true,
			true, true, true, true, true,
		}

		expr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("name"),
			Expr.Like,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, sql),
		)

		maskAny(t, src, expr, expected)
	})
}

func TestLikeSingleUnderscore(t *testing.T) {

	t.Run("name is exactly 5 characters (_____)", func(t *testing.T) {
		src := basicProject()
		sql := "_____"

		// Alice, David, Grace
		expected := []bool{
			true, // Alice (5)
			false,
			false,
			true, // David (5)
			false,
			true, // Frank (5)
			true, // Grace (5)
			false,
			false,
			false,
		}

		expr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("name"),
			Expr.Like,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, sql),
		)

		maskAny(t, src, expr, expected)
	})

	t.Run("name starts with H and length is 6 (H_____)", func(t *testing.T) {
		src := basicProject()
		sql := "H_____"

		// Hannah is 6 letters
		expected := []bool{
			false, false, false, false, false,
			false, false,
			true, // Hannah
			false, false,
		}

		expr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("name"),
			Expr.Like,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, sql),
		)

		maskAny(t, src, expr, expected)
	})

	t.Run("fourth letter is r (___r%)", func(t *testing.T) {
		src := basicProject()
		sql := "___r%"

		// Charlie → C h a r …
		expected := []bool{
			false, false,
			true, // Charlie
			false, false,
			false, false, false, false, false,
		}

		expr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("name"),
			Expr.Like,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, sql),
		)

		maskAny(t, src, expr, expected)
	})
}

func TestLikeMixedWildcards(t *testing.T) {

	t.Run("starts with C and exactly 7 chars (C______)", func(t *testing.T) {
		src := basicProject()
		sql := "C______"

		// Charlie (7 letters)
		expected := []bool{
			false, false,
			true, // Charlie
			false, false,
			false, false, false, false, false,
		}

		expr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("name"),
			Expr.Like,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, sql),
		)

		maskAny(t, src, expr, expected)
	})

	t.Run("ends with ake (_ake)", func(t *testing.T) {
		src := basicProject()
		sql := "_ake"

		// Jake → J a k e, matches _ake
		expected := []bool{
			false, false, false, false, false,
			false, false, false, false,
			true, // Jake
		}

		expr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("name"),
			Expr.Like,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, sql),
		)

		maskAny(t, src, expr, expected)
	})

	t.Run("starts with H and contains ah (H%ah%)", func(t *testing.T) {
		src := basicProject()
		sql := "H%ah%"

		// Hannah contains "ah" twice
		expected := []bool{
			false, false, false, false, false,
			false, false,
			true, // Hannah
			false, false,
		}

		expr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("name"),
			Expr.Like,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, sql),
		)

		maskAny(t, src, expr, expected)
	})
}

func TestLikeEdgeCases(t *testing.T) {

	t.Run("empty pattern matches nothing", func(t *testing.T) {
		src := basicProject()
		sql := ""

		expected := []bool{
			false, false, false, false, false,
			false, false, false, false, false,
		}

		expr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("name"),
			Expr.Like,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, sql),
		)

		maskAny(t, src, expr, expected)
	})

	t.Run("no names end with zz (%zz)", func(t *testing.T) {
		src := basicProject()
		sql := "%zz"

		expected := []bool{
			false, false, false, false, false,
			false, false, false, false, false,
		}

		expr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("name"),
			Expr.Like,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, sql),
		)

		maskAny(t, src, expr, expected)
	})

	t.Run("single underscore (_) matches 1-char names only", func(t *testing.T) {
		src := basicProject()
		sql := "_"

		expected := []bool{
			false, false, false, false, false,
			false, false, false, false, false,
		}

		expr := Expr.NewBinaryExpr(
			Expr.NewColumnResolve("name"),
			Expr.Like,
			Expr.NewLiteralResolve(arrow.BinaryTypes.String, sql),
		)

		maskAny(t, src, expr, expected)
	})
}
