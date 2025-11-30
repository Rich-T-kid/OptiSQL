package filter

import (
	"errors"
	"io"
	"opti-sql-go/Expr"
	"opti-sql-go/operators/project"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
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
func generateTestColumnsDistinct() ([]string, []any) {
	names := []string{
		"city",
		"state",
		"product",
	}
	columns := []any{
		// city - lots of repeated values
		[]string{
			"Boston", "Boston", "New York", "Boston", "Chicago",
			"New York", "Boston", "Chicago", "New York", "Boston",
			"Chicago", "Boston", "New York", "Chicago", "Boston",
		},
		// state - corresponds to cities
		[]string{
			"MA", "MA", "NY", "MA", "IL",
			"NY", "MA", "IL", "NY", "MA",
			"IL", "MA", "NY", "IL", "MA",
		},
		// product - repeated products
		[]string{
			"Laptop", "Phone", "Laptop", "Mouse", "Laptop",
			"Phone", "Laptop", "Phone", "Tablet", "Mouse",
			"Laptop", "Phone", "Laptop", "Tablet", "Mouse",
		},
	}
	return names, columns
}
func basicProject() *project.InMemorySource {
	names, col := generateTestColumns()
	v, _ := project.NewInMemoryProjectExec(names, col)
	return v
}
func distinctProject() *project.InMemorySource {
	names, col := generateTestColumnsDistinct()
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

// Distinct test cases

func TestDistinctInit(t *testing.T) {
	t.Run("distinct init and interface check", func(t *testing.T) {
		proj := distinctProject()
		exprs := []Expr.Expression{
			Expr.NewColumnResolve("city"),
		}
		distinctExec, err := NewDistinctExec(proj, exprs)
		if err != nil {
			t.Fatalf("unexpected error creating new distinct operator")
		}
		s := distinctExec.Schema()
		if !s.Equal(proj.Schema()) {
			t.Fatalf("distinct schema should be the exact same as input but recieved %v instead of %v", s, proj.Schema())
		}
		t.Logf("distinct operator %v\n", distinctExec)
		if err := distinctExec.Close(); err != nil {
			t.Fatalf("unexpected error occured closing operator %v\n", err)
		}
		distinctExec.done = true
		_, err = distinctExec.Next(3)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected io.EOF but got %v\n", err)
		}
	})
	t.Run("Basic Next operator test", func(t *testing.T) {
		proj := distinctProject()
		exprs := []Expr.Expression{
			Expr.NewColumnResolve("city"),
		}
		distinctExec, err := NewDistinctExec(proj, exprs)
		if err != nil {
			t.Fatalf("unexpected error creating new distinct operator")
		}
		rc, err := distinctExec.Next(5)
		if err != nil {
			t.Fatalf("error occured grabbing next values from distinct operator %v", err)
		}
		t.Logf("rc:\t%v\n", rc.PrettyPrint())

	})
	t.Run("Basic Next operator test | several distinct columns", func(t *testing.T) {
		proj := distinctProject()
		exprs := []Expr.Expression{
			Expr.NewColumnResolve("city"),
			Expr.NewColumnResolve("state"),
		}
		distinctExec, err := NewDistinctExec(proj, exprs)
		if err != nil {
			t.Fatalf("unexpected error creating new distinct operator")
		}
		rc, err := distinctExec.Next(5)
		if err != nil {
			t.Fatalf("error occured grabbing next values from distinct operator %v", err)
		}
		t.Logf("rc:\t%v\n", rc.PrettyPrint())

	})
}
func TestDistinctNext(t *testing.T) {
	t.Run("return limited columns", func(t *testing.T) {
		proj := distinctProject()
		exprs := []Expr.Expression{
			Expr.NewColumnResolve("city"),
			Expr.NewColumnResolve("state"),
		}
		distinctExec, err := NewDistinctExec(proj, exprs)
		if err != nil {
			t.Fatalf("unexpected error creating new distinct operator")
		}
		batchsize := 1
		count := 0
		for {
			rc, err := distinctExec.Next(uint16(batchsize))
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatalf("error occured grabbing next values from distinct operator %v", err)
			}
			t.Logf("\t%v\n", rc.PrettyPrint())
			if rc.RowCount != uint64(batchsize) {
				t.Fatalf("expected record batch of size %d but got %d", batchsize, rc.RowCount)
			}
			count += int(rc.RowCount)
		}
		// distinctProject has 3 distinct (city,state) combinations
		if count != 3 {
			t.Fatalf("expected total distinct rows 3, got %d", count)
		}
	})

	t.Run("single column distinct returns expected order", func(t *testing.T) {
		proj := distinctProject()
		exprs := []Expr.Expression{
			Expr.NewColumnResolve("city"),
		}
		distinctExec, err := NewDistinctExec(proj, exprs)
		if err != nil {
			t.Fatalf("unexpected error creating new distinct operator")
		}
		// request all in one go
		rc, err := distinctExec.Next(10)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if rc.RowCount != 3 {
			t.Fatalf("expected 3 distinct cities, got %d", rc.RowCount)
		}
		// Expect first-seen order: Boston, New York, Chicago
		cityArr := rc.Columns[0].(*array.String)
		expect := []string{"Boston", "New York", "Chicago"}
		for i := 0; i < int(rc.RowCount); i++ {
			if cityArr.Value(i) != expect[i] {
				t.Fatalf("expected city %s at idx %d, got %s", expect[i], i, cityArr.Value(i))
			}
		}
		for _, c := range rc.Columns {
			c.Release()
		}
	})

	t.Run("Next returns EOF after consumption and Close works", func(t *testing.T) {
		proj := distinctProject()
		exprs := []Expr.Expression{
			Expr.NewColumnResolve("city"),
			Expr.NewColumnResolve("state"),
		}
		distinctExec, err := NewDistinctExec(proj, exprs)
		if err != nil {
			t.Fatalf("unexpected error creating new distinct operator")
		}
		// consume all
		_, err = distinctExec.Next(10)
		if err != nil && !errors.Is(err, io.EOF) {
			print(1)
			// it's ok if we got results; call Next again until EOF
		}
		// subsequent Next should return EOF
		_, err = distinctExec.Next(1)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF after consuming distinct results, got %v", err)
		}
		if err := distinctExec.Close(); err != nil {
			t.Fatalf("unexpected error on Close: %v", err)
		}
	})
}

func TestJoinArrays(t *testing.T) {
	mem := memory.NewGoAllocator()

	t.Run("first array nil or empty - returns second", func(t *testing.T) {
		builder := array.NewInt32Builder(mem)
		defer builder.Release()
		builder.AppendValues([]int32{1, 2, 3}, nil)
		a2 := builder.NewArray()
		defer a2.Release()

		// Test with nil
		result, err := joinArrays(nil, a2, mem)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 3 {
			t.Fatalf("expected length 3, got %d", result.Len())
		}

		// Test with empty array
		emptyBuilder := array.NewInt32Builder(mem)
		defer emptyBuilder.Release()
		a1Empty := emptyBuilder.NewArray()
		defer a1Empty.Release()

		result, err = joinArrays(a1Empty, a2, mem)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 3 {
			t.Fatalf("expected length 3, got %d", result.Len())
		}
	})

	t.Run("second array nil or empty - returns first", func(t *testing.T) {
		builder := array.NewInt32Builder(mem)
		defer builder.Release()
		builder.AppendValues([]int32{4, 5, 6}, nil)
		a1 := builder.NewArray()
		defer a1.Release()

		// Test with nil
		result, err := joinArrays(a1, nil, mem)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 3 {
			t.Fatalf("expected length 3, got %d", result.Len())
		}

		// Test with empty array
		emptyBuilder := array.NewInt32Builder(mem)
		defer emptyBuilder.Release()
		a2Empty := emptyBuilder.NewArray()
		defer a2Empty.Release()

		result, err = joinArrays(a1, a2Empty, mem)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 3 {
			t.Fatalf("expected length 3, got %d", result.Len())
		}
	})

	t.Run("both arrays have data - concatenates", func(t *testing.T) {
		builder1 := array.NewInt32Builder(mem)
		defer builder1.Release()
		builder1.AppendValues([]int32{1, 2, 3}, nil)
		a1 := builder1.NewArray()
		defer a1.Release()

		builder2 := array.NewInt32Builder(mem)
		defer builder2.Release()
		builder2.AppendValues([]int32{4, 5, 6}, nil)
		a2 := builder2.NewArray()
		defer a2.Release()

		result, err := joinArrays(a1, a2, mem)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 6 {
			t.Fatalf("expected length 6, got %d", result.Len())
		}

		// Verify concatenated values
		int32Result := result.(*array.Int32)
		expectedValues := []int32{1, 2, 3, 4, 5, 6}
		for i := 0; i < int32Result.Len(); i++ {
			if int32Result.Value(i) != expectedValues[i] {
				t.Fatalf("at index %d: expected %d, got %d", i, expectedValues[i], int32Result.Value(i))
			}
		}
	})
}
