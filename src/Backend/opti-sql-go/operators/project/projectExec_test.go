package project

import (
	"errors"
	"io"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
)

func TestProjectExecInit(t *testing.T) {
	// Simple passing test
}

func TestProjectPrune(t *testing.T) {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64},
		{Name: "country", Type: arrow.BinaryTypes.String},
		{Name: "email", Type: arrow.BinaryTypes.String},
		{Name: "signup_date", Type: arrow.FixedWidthTypes.Date32},
	}
	schema := arrow.NewSchema(fields, nil)
	t.Run("validate prune 1", func(t *testing.T) {
		keepCols := []string{"id", "name", "email"}
		newSchema, err := prunedSchema(schema, keepCols)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if newSchema.NumFields() != len(keepCols) {
			t.Fatalf("expected %d fields, got %d", len(keepCols), newSchema.NumFields())
		}
		for i, field := range newSchema.Fields() {
			if field.Name != keepCols[i] {
				t.Fatalf("expected field %s, got %s", keepCols[i], field.Name)
			}
		}
		t.Logf("%s\n", newSchema)
	})
	t.Run("validate prune 2", func(t *testing.T) {
		keeptCols := []string{"age", "country", "signup_date"}
		newSchema, err := prunedSchema(schema, keeptCols)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if newSchema.NumFields() != len(keeptCols) {
			t.Fatalf("expected %d fields, got %d", len(keeptCols), newSchema.NumFields())
		}
		for i, field := range newSchema.Fields() {
			if field.Name != keeptCols[i] {
				t.Fatalf("expected field %s, got %s", keeptCols[i], field.Name)
			}
		}
		t.Logf("%s\n", newSchema)

	})
	t.Run("prune non-existant column", func(t *testing.T) {
		keepCols := []string{"id", "non_existing_column"}
		_, err := prunedSchema(schema, keepCols)
		if err == nil {
			t.Fatalf("expected error for non-existing column, got nil")
		}
		if !errors.Is(err, ErrProjectColumnNotFound) {
			t.Fatalf("expected ErrProjectColumnNotFound, got %v", err)
		}

	})
	t.Run("Prune empty input keepcols", func(t *testing.T) {
		keepCols := []string{}
		_, err := prunedSchema(schema, keepCols)
		if err == nil {
			t.Fatalf("expected error for empty keepcols, got nil")
		}
		if !errors.Is(err, ErrEmptyColumnsToProject) {
			t.Fatalf("expected ErrEmptyColumnsToProject, got %v", err)
		}
	})

}
func TestProjectExec(t *testing.T) {
	names, col := generateTestColumns()
	memorySource, err := NewInMemoryProjectExec(names, col)
	if err != nil {
		t.Fatalf("failed to create in memory source: %v", err)
	}
	t.Logf("original schema %v\n", memorySource.Schema())
	projectExec, err := NewProjectExec([]string{"id", "name", "age"}, memorySource)
	if err != nil {
		t.Fatalf("failed to create project exec: %v", err)
	}
	rc, err := projectExec.Next(3)
	if err != nil {
		t.Fatalf("failed to get next record batch: %v", err)
	}
	t.Logf("rc:%v\n", rc)

}

// NewProjectExec, pruned schema errors and iteration behavior.
func TestProjectExec_Subtests(t *testing.T) {
	names, cols := generateTestColumns()

	t.Run("ValidProjection", func(t *testing.T) {
		memSrc, err := NewInMemoryProjectExec(names, cols)
		if err != nil {
			t.Fatalf("failed to create in memory source: %v", err)
		}
		projCols := []string{"id", "name", "age"}
		projExec, err := NewProjectExec(projCols, memSrc)
		if err != nil {
			t.Fatalf("failed to create project exec: %v", err)
		}
		rb, err := projExec.Next(4)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if rb == nil {
			t.Fatalf("expected a record batch, got nil")
		}
		if len(rb.Columns) != len(projCols) {
			t.Fatalf("expected %d columns, got %d", len(projCols), len(rb.Columns))
		}
		for _, c := range rb.Columns {
			c.Release()
		}
	})

	t.Run("EmptyColumns", func(t *testing.T) {
		memSrc, err := NewInMemoryProjectExec(names, cols)
		if err != nil {
			t.Fatalf("failed to create in memory source: %v", err)
		}
		_, err = NewProjectExec([]string{}, memSrc)
		if err == nil {
			t.Fatalf("expected error for empty project columns, got nil")
		}
		if !errors.Is(err, ErrEmptyColumnsToProject) {
			t.Fatalf("expected ErrEmptyColumnsToProject, got %v", err)
		}
	})

	t.Run("NonExistentColumn", func(t *testing.T) {
		memSrc, err := NewInMemoryProjectExec(names, cols)
		if err != nil {
			t.Fatalf("failed to create in memory source: %v", err)
		}
		_, err = NewProjectExec([]string{"id", "nope"}, memSrc)
		if err == nil {
			t.Fatalf("expected error for non-existent column, got nil")
		}
		if !errors.Is(err, ErrProjectColumnNotFound) {
			t.Fatalf("expected ErrProjectColumnNotFound, got %v", err)
		}
	})

	t.Run("SchemaMatch", func(t *testing.T) {
		memSrc, err := NewInMemoryProjectExec(names, cols)
		if err != nil {
			t.Fatalf("failed to create in memory source: %v", err)
		}
		projCols := []string{"id", "name"}
		projExec, err := NewProjectExec(projCols, memSrc)
		if err != nil {
			t.Fatalf("failed to create project exec: %v", err)
		}
		execSchema := projExec.Schema()
		pruned, err := prunedSchema(memSrc.Schema(), projCols)
		if err != nil {
			t.Fatalf("prunedSchema failed: %v", err)
		}
		if !execSchema.Equal(pruned) {
			t.Fatalf("expected exec schema %v, got %v", pruned, execSchema)
		}
		_ = projExec
	})

	t.Run("IterateUntilEOF", func(t *testing.T) {
		memSrc, err := NewInMemoryProjectExec(names, cols)
		if err != nil {
			t.Fatalf("failed to create in memory source: %v", err)
		}
		projExec, err := NewProjectExec([]string{"id", "name"}, memSrc)
		if err != nil {
			t.Fatalf("failed to create project exec: %v", err)
		}
		total := 0
		batches := 0
		for {
			rb, err := projExec.Next(3)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatalf("Next returned unexpected error: %v", err)
			}
			if rb == nil {
				t.Fatalf("expected record batch, got nil")
			}
			total += int(rb.Columns[0].Len())
			batches++
			for _, c := range rb.Columns {
				c.Release()
			}
		}
		if batches == 0 {
			t.Fatalf("expected at least 1 batch, got 0")
		}
	})

	t.Run("SingleColumnProjection", func(t *testing.T) {
		memSrc, err := NewInMemoryProjectExec(names, cols)
		if err != nil {
			t.Fatalf("failed to create in memory source: %v", err)
		}
		projExec, err := NewProjectExec([]string{"department"}, memSrc)
		if err != nil {
			t.Fatalf("failed to create project exec: %v", err)
		}
		total := 0
		for {
			rb, err := projExec.Next(5)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatalf("Next returned unexpected error: %v", err)
			}
			if len(rb.Columns) != 1 {
				t.Fatalf("expected 1 column, got %d", len(rb.Columns))
			}
			total += int(rb.Columns[0].Len())
			for _, c := range rb.Columns {
				c.Release()
			}
		}
	})
	t.Run("Check Close", func(t *testing.T) {
		memSrc, err := NewInMemoryProjectExec(names, cols)
		if err != nil {
			t.Fatalf("failed to create in memory source: %v", err)
		}
		projExec, err := NewProjectExec([]string{"department"}, memSrc)
		if err != nil {
			t.Fatalf("failed to create project exec: %v", err)
		}
		err = projExec.Close()
		if err != nil {
			t.Fatalf("expected no error on Close, got %v", err)
		}

	})
	t.Run("Empty ProjectFilter", func(t *testing.T) {
		memSrc, err := NewInMemoryProjectExec(names, cols)
		if err != nil {
			t.Fatalf("failed to create in memory source: %v", err)
		}
		_, _, err = ProjectSchemaFilterDown(memSrc.Schema(), memSrc.columns, []string{}...)
		if err == nil {
			t.Fatalf("expected error for empty project filter, got nil")
		}
		if !errors.Is(err, ErrEmptyColumnsToProject) {
			t.Fatalf("expected ErrEmptyColumnsToProject, got %v", err)
		}
		_, _, err = ProjectSchemaFilterDown(memSrc.Schema(), memSrc.columns, []string{"This column doesnt exist"}...)
		if err == nil {
			t.Fatalf("expected error for non-existent column in project filter, got nil")
		}
		if !errors.Is(err, ErrProjectColumnNotFound) {
			t.Fatalf("expected ErrProjectColumnNotFound, got %v", err)
		}

	})

}
