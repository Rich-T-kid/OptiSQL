package filter

import (
	"errors"
	"io"
	"opti-sql-go/operators/project"
	"testing"
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
			t.Fatalf("was expecting io.EOF but recieved %v", err)
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
		lim.Close()
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
		lim.Close()
	})

	t.Run("AfterEOFAlwaysEOF", func(t *testing.T) {
		memSrc3, _ := project.NewInMemoryProjectExec(names, cols)
		lim, _ := NewLimitExec(memSrc3, 2)

		lim.Next(3) // exhaust

		_, err := lim.Next(1)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected EOF, got %v", err)
		}
		lim.Close()
	})
}
