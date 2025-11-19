package project

import (
	"errors"
	"io"
	"opti-sql-go/Expr"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

func TestProjectExec_Init(t *testing.T) {
	names, cols := generateTestColumns()
	memSrc, err := NewInMemoryProjectExec(names, cols)
	if err != nil {
		t.Fatalf("failed to create in memory source: %v", err)
	}

	exprs := []Expr.Expression{
		&Expr.ColumnResolve{Name: "id"},
		&Expr.ColumnResolve{Name: "name"},
	}

	proj, err := NewProjectExec(memSrc, exprs)
	if err != nil {
		t.Fatalf("failed to create project exec: %v", err)
	}

	schema := proj.Schema()
	if schema.NumFields() != len(exprs) {
		t.Fatalf("expected %d fields, got %d", len(exprs), schema.NumFields())
	}
}

func TestProjectExec_BasicColumns(t *testing.T) {
	names, cols := generateTestColumns()
	memSrc, _ := NewInMemoryProjectExec(names, cols)

	exprs := []Expr.Expression{
		&Expr.ColumnResolve{Name: "id"},
		&Expr.ColumnResolve{Name: "name"},
	}

	projExec, err := NewProjectExec(memSrc, exprs)
	if err != nil {
		t.Fatalf("failed to create project exec: %v", err)
	}

	rb, err := projExec.Next(3)
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if len(rb.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(rb.Columns))
	}

	for _, c := range rb.Columns {
		c.Release()
	}
	t.Logf("record batch: %+v", rb)
}

func TestProjectExec_Alias(t *testing.T) {
	names, cols := generateTestColumns()
	memSrc, _ := NewInMemoryProjectExec(names, cols)

	exprs := []Expr.Expression{
		&Expr.Alias{
			Expr: &Expr.ColumnResolve{Name: "id"},
			Name: "identifier",
		},
		&Expr.Alias{
			Expr: &Expr.ColumnResolve{Name: "name"},
			Name: "full_name",
		},
	}

	projExec, err := NewProjectExec(memSrc, exprs)
	if err != nil {
		t.Fatalf("failed to create project exec: %v", err)
	}

	schema := projExec.Schema()

	if schema.Field(0).Name != "identifier" {
		t.Fatalf("expected identifier, got %s", schema.Field(0).Name)
	}
	if schema.Field(1).Name != "full_name" {
		t.Fatalf("expected full_name, got %s", schema.Field(1).Name)
	}
	t.Logf("schema %v\n", schema)
}

func TestProjectExec_Literal(t *testing.T) {
	names, cols := generateTestColumns()
	memSrc, _ := NewInMemoryProjectExec(names, cols)

	lit := &Expr.LiteralResolve{
		Type:  arrow.PrimitiveTypes.Int64,
		Value: int64(99),
	}
	projExec, err := NewProjectExec(memSrc, []Expr.Expression{lit})
	if err != nil {
		t.Fatalf("failed to init project exec: %v", err)
	}

	rb, err := projExec.Next(5)
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	col := rb.Columns[0].(*array.Int64)
	for i := 0; i < col.Len(); i++ {
		if col.Value(i) != 99 {
			t.Fatalf("expected 99, got %d", col.Value(i))
		}
	}

	for _, c := range rb.Columns {
		c.Release()
	}
}

func TestProjectExec_BinaryAdd(t *testing.T) {
	names, cols := generateTestColumns()
	memSrc, _ := NewInMemoryProjectExec(names, cols)

	expr := &Expr.BinaryExpr{
		Left:  &Expr.ColumnResolve{Name: "age"},
		Op:    Expr.Addition,
		Right: &Expr.LiteralResolve{Type: arrow.PrimitiveTypes.Int64, Value: int64(1)},
	}

	projExec, err := NewProjectExec(memSrc, []Expr.Expression{expr})
	if err != nil {
		t.Fatalf("failed: %v", err)
	}

	rb, err := projExec.Next(3)
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}

	if len(rb.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(rb.Columns))
	}

	for _, c := range rb.Columns {
		c.Release()
	}
	t.Logf("column: %+v", rb.Columns[0])
}

// TODO: once your implement the other operators this test will fail
func TestUnimplemntedOperators(t *testing.T) {
	names, cols := generateTestColumns()
	memSrc, _ := NewInMemoryProjectExec(names, cols)
	for i := Expr.Equal; i <= Expr.Or; i++ {
		br := Expr.NewBinaryExpr(Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int16, int16(10)), i, Expr.NewLiteralResolve(arrow.PrimitiveTypes.Int16, int16(5)))

		proj, err := NewProjectExec(memSrc, []Expr.Expression{br})
		if err != nil {
			t.Fatalf("failed to create project exec: %v", err)
		}
		_, err = proj.Next(1)
		if err == nil {
			t.Fatalf("expected error for unimplemented operator %d, got nil", i)

		}
		t.Logf("error: %v", err)
	}
}

func TestProjectExec_IterateEOF(t *testing.T) {
	names, cols := generateTestColumns()
	memSrc, _ := NewInMemoryProjectExec(names, cols)

	exprs := []Expr.Expression{
		&Expr.ColumnResolve{Name: "id"},
	}

	projExec, _ := NewProjectExec(memSrc, exprs)

	count := 0
	for {
		rb, err := projExec.Next(2)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		count += rb.Columns[0].Len()
		for _, c := range rb.Columns {
			c.Release()
		}
	}

	if count == 0 {
		t.Fatalf("expected some rows, got 0")
	}
}

func TestProjectExec_Close(t *testing.T) {
	names, cols := generateTestColumns()
	memSrc, _ := NewInMemoryProjectExec(names, cols)

	exprs := []Expr.Expression{
		&Expr.ColumnResolve{Name: "id"},
	}

	projExec, _ := NewProjectExec(memSrc, exprs)
	if err := projExec.Close(); err != nil {
		t.Fatalf("close returned error: %v", err)
	}
}
func TestEOFBehavior(t *testing.T) {
	names, cols := []string{"name"}, []any{
		[]string{"richard"},
	}

	memSrc, err := NewInMemoryProjectExec(names, cols)
	if err != nil {
		t.Fatalf("failed to create in memory source: %v", err)
	}
	proj, err := NewProjectExec(memSrc, []Expr.Expression{&Expr.ColumnResolve{Name: "name"}})
	if err != nil {
		t.Fatalf("failed to create project exec: %v", err)
	}
	rc, err := proj.Next(1)
	if err != nil {
		t.Fatalf("unexpected error on first Next: %v", err)
	}
	nameCol, ok := rc.Columns[0].(*array.String)
	if !ok {
		t.Fatalf("expected String array, got %T", rc.Columns[0])
	}
	if nameCol.Value(0) != "richard" {
		t.Fatalf("expected 'richard', got '%s'", nameCol.Value(0))
	}

	_, err = proj.Next(10)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF error on second Next, got %v", err)
	}
}

func TestPruneSchema_cvg(t *testing.T) {
	// build a sample schema
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)

	t.Run("EmptyKeepCols_ReturnsErrAndEmptySchema", func(t *testing.T) {
		s, err := prunedSchema(schema, []string{})
		if err == nil {
			t.Fatalf("expected ErrEmptyColumnsToProject, got nil")
		}
		if !errors.Is(err, ErrEmptyColumnsToProject) {
			t.Fatalf("expected ErrEmptyColumnsToProject, got %v", err)
		}
		if s == nil {
			t.Fatalf("expected non-nil schema even on empty columns")
		}
		if s.NumFields() != 0 {
			t.Fatalf("expected 0 fields, got %d", s.NumFields())
		}
	})

	t.Run("ValidKeepCols_PreservesOrderAndTypes", func(t *testing.T) {
		keep := []string{"name", "id"}
		s, err := prunedSchema(schema, keep)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s.NumFields() != 2 {
			t.Fatalf("expected 2 fields, got %d", s.NumFields())
		}
		if s.Field(0).Name != "name" || s.Field(0).Type.ID() != arrow.STRING {
			t.Fatalf("field 0 mismatch, got %v", s.Field(0))
		}
		if s.Field(1).Name != "id" || s.Field(1).Type.ID() != arrow.INT32 {
			t.Fatalf("field 1 mismatch, got %v", s.Field(1))
		}
	})

	t.Run("MissingColumn_ReturnsErrProjectColumnNotFound", func(t *testing.T) {
		_, err := prunedSchema(schema, []string{"missing_col"})
		if err == nil {
			t.Fatalf("expected ErrProjectColumnNotFound, got nil")
		}
		if !errors.Is(err, ErrProjectColumnNotFound) {
			t.Fatalf("expected ErrProjectColumnNotFound, got %v", err)
		}
	})

}
