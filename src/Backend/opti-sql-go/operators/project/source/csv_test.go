package source

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

const csvFilePath = "../../../../test_data/csv/Mental_Health_and_Social_Media_Balance_Dataset.csv"

//const csvFilePathLarger = "../../../../test_data/csv/stats.csv"

func getTestFile() *os.File {
	v, err := os.Open(csvFilePath)
	if err != nil {
		panic(err)
	}
	return v
}

/*
	func getTestFile2() *os.File {
		v, err := os.Open(csvFilePathLarger)
		if err != nil {
			panic(err)
		}
		return v
	}
*/
func TestCsvInit(t *testing.T) {
	v := getTestFile()
	defer func() {
		if err := v.Close(); err != nil {
			t.Fatalf("failed to close: %v", err)
		}
	}()
	p, err := NewProjectCSVLeaf(v)
	if err != nil {
		t.Errorf("Failed to create ProjectCSVLeaf: %v", err)
	}
	fmt.Printf("schema -> %v\n", p.schema)
	fmt.Printf("columns Mapping -> %v\n", p.colPosition)
}
func TestProjectComponents(t *testing.T) {
	v := getTestFile()
	defer func() {
		if err := v.Close(); err != nil {
			t.Fatalf("failed to close: %v", err)
		}
	}()
	p, err := NewProjectCSVLeaf(v)
	if err != nil {
		t.Errorf("Failed to create ProjectCSVLeaf: %v", err)
	}
	if p.schema == nil {
		t.Errorf("Schema is nil")
	}
	if len(p.colPosition) == 0 {
		t.Errorf("Column position mapping is empty")
	}
}
func TestCsvNext(t *testing.T) {
	v := getTestFile()
	defer func() {
		if err := v.Close(); err != nil {
			t.Fatalf("failed to close: %v", err)
		}
	}()

	csvLeaf, err := NewProjectCSVLeaf(v)
	if err != nil {
		t.Errorf("Failed to create ProjectCSVLeaf: %v", err)
	}
	rBatch, err := csvLeaf.Next(10)
	if err != nil {
		t.Errorf("Failed to read next batch from CSV: %v", err)
	}
	fmt.Printf("Batch: %v\n", rBatch)
}

// TestParseDataType tests every branch of the parseDataType function
func TestParseDataType(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected arrow.DataType
	}{
		// Empty and NULL cases
		{
			name:     "Empty string",
			input:    "",
			expected: arrow.BinaryTypes.String,
		},
		{
			name:     "NULL uppercase",
			input:    "NULL",
			expected: arrow.BinaryTypes.String,
		},
		{
			name:     "NULL lowercase",
			input:    "null",
			expected: arrow.BinaryTypes.String,
		},
		{
			name:     "NULL mixed case",
			input:    "NuLl",
			expected: arrow.BinaryTypes.String,
		},
		{
			name:     "Empty string with whitespace",
			input:    "   ",
			expected: arrow.BinaryTypes.String,
		},
		{
			name:     "NULL with whitespace",
			input:    "  NULL  ",
			expected: arrow.BinaryTypes.String,
		},

		// Boolean cases
		{
			name:     "Boolean true",
			input:    "true",
			expected: arrow.FixedWidthTypes.Boolean,
		},
		{
			name:     "Boolean false",
			input:    "false",
			expected: arrow.FixedWidthTypes.Boolean,
		},
		{
			name:     "Boolean true with whitespace",
			input:    "  true  ",
			expected: arrow.FixedWidthTypes.Boolean,
		},
		{
			name:     "Boolean false with whitespace",
			input:    "  false  ",
			expected: arrow.FixedWidthTypes.Boolean,
		},

		// Integer cases
		{
			name:     "Positive integer",
			input:    "123",
			expected: arrow.PrimitiveTypes.Int64,
		},
		{
			name:     "Negative integer",
			input:    "-456",
			expected: arrow.PrimitiveTypes.Int64,
		},
		{
			name:     "Zero",
			input:    "0",
			expected: arrow.PrimitiveTypes.Int64,
		},
		{
			name:     "Integer with whitespace",
			input:    "  789  ",
			expected: arrow.PrimitiveTypes.Int64,
		},

		// Float cases
		{
			name:     "Positive float",
			input:    "3.14",
			expected: arrow.PrimitiveTypes.Float64,
		},
		{
			name:     "Negative float",
			input:    "-2.71",
			expected: arrow.PrimitiveTypes.Float64,
		},
		{
			name:     "Float with leading zero",
			input:    "0.5",
			expected: arrow.PrimitiveTypes.Float64,
		},
		{
			name:     "Float with trailing zero",
			input:    "1.0",
			expected: arrow.PrimitiveTypes.Float64,
		},
		{
			name:     "Float with whitespace",
			input:    "  9.99  ",
			expected: arrow.PrimitiveTypes.Float64,
		},
		{
			name:     "Scientific notation",
			input:    "1.23e10",
			expected: arrow.PrimitiveTypes.Float64,
		},

		// String fallback cases
		{
			name:     "Regular string",
			input:    "hello",
			expected: arrow.BinaryTypes.String,
		},
		{
			name:     "String with spaces",
			input:    "hello world",
			expected: arrow.BinaryTypes.String,
		},
		{
			name:     "String with numbers",
			input:    "abc123",
			expected: arrow.BinaryTypes.String,
		},
		{
			name:     "Boolean-like but not exact",
			input:    "True",
			expected: arrow.BinaryTypes.String,
		},
		{
			name:     "Boolean-like but not exact 2",
			input:    "FALSE",
			expected: arrow.BinaryTypes.String,
		},
		{
			name:     "Invalid number",
			input:    "12.34.56",
			expected: arrow.BinaryTypes.String,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseDataType(tt.input)
			if result != tt.expected {
				t.Errorf("parseDataType(%q) = %v, expected %v", tt.input, result, tt.expected)
			}
		})
	}
}

// TestParseHeader tests the parseHeader function
func TestParseHeader(t *testing.T) {
	t.Run("Valid header with all data types", func(t *testing.T) {
		csvData := `id,name,age,salary,active
123,John,30,50000.50,true`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		// Check schema was created
		if proj.schema == nil {
			t.Fatal("Schema is nil")
		}

		// Check correct number of fields
		fields := proj.schema.Fields()
		if len(fields) != 5 {
			t.Errorf("Expected 5 fields, got %d", len(fields))
		}

		// Check field names and types
		expectedFields := map[string]arrow.DataType{
			"id":     arrow.PrimitiveTypes.Int64,
			"name":   arrow.BinaryTypes.String,
			"age":    arrow.PrimitiveTypes.Int64,
			"salary": arrow.PrimitiveTypes.Float64,
			"active": arrow.FixedWidthTypes.Boolean,
		}

		for _, field := range fields {
			expectedType, exists := expectedFields[field.Name]
			if !exists {
				t.Errorf("Unexpected field name: %s", field.Name)
				continue
			}
			if field.Type != expectedType {
				t.Errorf("Field %s: expected type %v, got %v", field.Name, expectedType, field.Type)
			}
			if !field.Nullable {
				t.Errorf("Field %s: expected nullable=true, got false", field.Name)
			}
		}

		// Check column position mapping
		if len(proj.colPosition) != 5 {
			t.Errorf("Expected 5 column positions, got %d", len(proj.colPosition))
		}

		expectedPositions := map[string]int{
			"id":     0,
			"name":   1,
			"age":    2,
			"salary": 3,
			"active": 4,
		}

		for name, expectedPos := range expectedPositions {
			actualPos, exists := proj.colPosition[name]
			if !exists {
				t.Errorf("Column position for %s not found", name)
				continue
			}
			if actualPos != expectedPos {
				t.Errorf("Column %s: expected position %d, got %d", name, expectedPos, actualPos)
			}
		}
	})

	t.Run("Header with NULL values", func(t *testing.T) {
		csvData := `col1,col2,col3
NULL,,value`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		fields := proj.schema.Fields()
		// All should be inferred as string
		for _, field := range fields {
			if field.Type != arrow.BinaryTypes.String {
				t.Errorf("Field %s: expected String type for NULL/empty value, got %v", field.Name, field.Type)
			}
		}
	})

	t.Run("Empty file - header only", func(t *testing.T) {
		csvData := `col1,col2`
		reader := strings.NewReader(csvData)
		_, err := NewProjectCSVLeaf(reader)
		if err == nil {
			t.Error("Expected error for CSV with header but no data rows")
		}
	})

	t.Run("Completely empty file", func(t *testing.T) {
		csvData := ``
		reader := strings.NewReader(csvData)
		_, err := NewProjectCSVLeaf(reader)
		if err == nil {
			t.Error("Expected error for completely empty CSV")
		}
	})
}

// TestNewProjectCSVLeaf tests the constructor
func TestNewProjectCSVLeaf(t *testing.T) {
	t.Run("Valid CSV initialization", func(t *testing.T) {
		csvData := `name,value
test,123`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		if proj == nil {
			t.Fatal("ProjectCSVLeaf is nil")
		}
		if proj.r == nil {
			t.Error("CSV reader is nil")
		}
		if proj.schema == nil {
			t.Error("Schema is nil")
		}
		if proj.colPosition == nil {
			t.Error("Column position map is nil")
		}
		if proj.done {
			t.Error("done flag should be false initially")
		}
	})

	t.Run("Error during header parsing", func(t *testing.T) {
		csvData := `only_header`
		reader := strings.NewReader(csvData)
		_, err := NewProjectCSVLeaf(reader)
		if err == nil {
			t.Error("Expected error when no data rows present")
		}
	})
}

// TestNextFunction tests the Next function comprehensively
func TestNextFunction(t *testing.T) {
	t.Run("Read single batch with all data types", func(t *testing.T) {
		csvData := `id,name,score,active
1,Alice,95.5,true
2,Bob,87.3,false
3,Charlie,92.1,true`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		batch, err := proj.Next(10)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		if batch == nil {
			t.Fatal("Batch is nil")
		}

		// Check schema
		if batch.Schema == nil {
			t.Fatal("Batch schema is nil")
		}

		// Check columns
		if len(batch.Columns) != 4 {
			t.Fatalf("Expected 4 columns, got %d", len(batch.Columns))
		}

		// Verify each column has 3 rows
		for i, col := range batch.Columns {
			if col.Len() != 3 {
				t.Errorf("Column %d: expected 3 rows, got %d", i, col.Len())
			}
		}
		fmt.Printf("col0: %v\n", batch.Columns[0])
		// Check Int64 column (id)
		idCol, ok := batch.Columns[0].(*array.Int64)
		if !ok {
			t.Errorf("Column 0 (id): expected *array.Int64, got %T", batch.Columns[0])
		} else {
			if idCol.Value(0) != 1 || idCol.Value(1) != 2 || idCol.Value(2) != 3 {
				t.Errorf("ID column values incorrect: got [%d, %d, %d]", idCol.Value(0), idCol.Value(1), idCol.Value(2))
			}
		}

		// Check String column (name)
		nameCol, ok := batch.Columns[1].(*array.String)
		if !ok {
			t.Errorf("Column 1 (name): expected *array.String, got %T", batch.Columns[1])
		} else {
			if nameCol.Value(0) != "Alice" || nameCol.Value(1) != "Bob" || nameCol.Value(2) != "Charlie" {
				t.Errorf("Name column values incorrect")
			}
		}

		// Check Float64 column (score)
		scoreCol, ok := batch.Columns[2].(*array.Float64)
		if !ok {
			t.Errorf("Column 2 (score): expected *array.Float64, got %T", batch.Columns[2])
		} else {
			if scoreCol.Value(0) != 95.5 || scoreCol.Value(1) != 87.3 || scoreCol.Value(2) != 92.1 {
				t.Errorf("Score column values incorrect")
			}
		}

		// Check Boolean column (active)
		activeCol, ok := batch.Columns[3].(*array.Boolean)
		if !ok {
			t.Errorf("Column 3 (active): expected *array.Boolean, got %T", batch.Columns[3])
		} else {
			if !activeCol.Value(0) || activeCol.Value(1) || !activeCol.Value(2) {
				t.Errorf("Active column values incorrect")
			}
		}
	})

	t.Run("Read with NULL values - Int64", func(t *testing.T) {
		csvData := `id,value
1,100
,200
3,`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		batch, err := proj.Next(10)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		// Check id column for NULLs
		idCol, ok := batch.Columns[0].(*array.Int64)
		if !ok {
			t.Fatalf("Column 0: expected *array.Int64, got %T", batch.Columns[0])
		}

		if !idCol.IsNull(1) {
			t.Error("Expected NULL at index 1 in id column")
		}
		if idCol.IsNull(0) || idCol.IsNull(2) {
			t.Error("Unexpected NULL in id column")
		}

		// Check value column for NULLs
		valueCol, ok := batch.Columns[1].(*array.Int64)
		if !ok {
			t.Fatalf("Column 1: expected *array.Int64, got %T", batch.Columns[1])
		}

		if !valueCol.IsNull(2) {
			t.Error("Expected NULL at index 2 in value column")
		}
	})

	t.Run("Read with NULL values - Float64", func(t *testing.T) {
		csvData := `price
99.99
NULL
`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		batch, err := proj.Next(10)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		priceCol, ok := batch.Columns[0].(*array.Float64)
		if !ok {
			t.Fatalf("Expected *array.Float64, got %T", batch.Columns[0])
		}

		if !priceCol.IsNull(1) || !priceCol.IsNull(2) {
			t.Error("Expected NULL values in price column")
		}
	})

	t.Run("Read with NULL values - String", func(t *testing.T) {
		csvData := `name
Alice
NULL
`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		batch, err := proj.Next(10)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		nameCol, ok := batch.Columns[0].(*array.String)
		if !ok {
			t.Fatalf("Expected *array.String, got %T", batch.Columns[0])
		}

		if !nameCol.IsNull(1) || !nameCol.IsNull(2) {
			t.Error("Expected NULL values in name column")
		}
	})

	t.Run("Read with NULL values - Boolean", func(t *testing.T) {
		csvData := `flag
true
NULL
false
`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		batch, err := proj.Next(10)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		flagCol, ok := batch.Columns[0].(*array.Boolean)
		if !ok {
			t.Fatalf("Expected *array.Boolean, got %T", batch.Columns[0])
		}
		fmt.Printf("flagCol : %v\n", flagCol)

		if !flagCol.IsNull(1) {
			t.Error("Expected NULL values in flag column")
		}
	})

	t.Run("Read multiple batches", func(t *testing.T) {
		csvData := `id
1
2
3
4
5
6`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		// First batch of 2
		batch1, err := proj.Next(2)
		if err != nil {
			t.Fatalf("First Next failed: %v", err)
		}
		if batch1.Columns[0].Len() != 2 {
			t.Errorf("First batch: expected 2 rows, got %d", batch1.Columns[0].Len())
		}

		// Second batch of 3
		batch2, err := proj.Next(3)
		if err != nil {
			t.Fatalf("Second Next failed: %v", err)
		}
		if batch2.Columns[0].Len() != 3 {
			t.Errorf("Second batch: expected 3 rows, got %d", batch2.Columns[0].Len())
		}

		// Third batch - should get remaining 1 row
		batch3, err := proj.Next(10)
		if err != nil {
			t.Fatalf("Third Next failed: %v", err)
		}
		if batch3.Columns[0].Len() != 1 {
			t.Errorf("Third batch: expected 1 row, got %d", batch3.Columns[0].Len())
		}

		// Fourth batch - should return EOF
		_, err = proj.Next(10)
		if err != io.EOF {
			t.Errorf("Expected EOF, got: %v", err)
		}
	})

	t.Run("Read exact batch size", func(t *testing.T) {
		csvData := `num
10
20
30`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		batch, err := proj.Next(3)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if batch.Columns[0].Len() != 3 {
			t.Errorf("Expected 3 rows, got %d", batch.Columns[0].Len())
		}

		// Next call should return EOF
		_, err = proj.Next(1)
		if err != io.EOF {
			t.Errorf("Expected EOF after reading all data, got: %v", err)
		}
	})

	t.Run("EOF on first Next call - empty data", func(t *testing.T) {
		csvData := `col1
val1`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		// Read the only row
		_, err = proj.Next(10)
		if err != nil {
			t.Fatalf("First Next failed: %v", err)
		}

		// Second call when no data remains and rowsRead == 0
		_, err = proj.Next(10)
		if err != io.EOF {
			t.Errorf("Expected EOF when no data left, got: %v", err)
		}

		// Verify done flag is set
		if !proj.done {
			t.Error("Expected done flag to be true after EOF")
		}
	})

	t.Run("Subsequent calls after done is set", func(t *testing.T) {
		csvData := `val
1`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		// Read all data
		_, _ = proj.Next(10)

		// Hit EOF and set done
		_, err = proj.Next(10)
		if err != io.EOF {
			t.Fatalf("Expected EOF, got: %v", err)
		}

		// Call again - should immediately return EOF due to done flag
		_, err = proj.Next(10)
		if err != io.EOF {
			t.Errorf("Expected EOF on subsequent call when done=true, got: %v", err)
		}
	})

	t.Run("Batch size of 1", func(t *testing.T) {
		csvData := `x
a
b
c`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		// Read one row at a time
		for i := 0; i < 3; i++ {
			batch, err := proj.Next(1)
			if err != nil {
				t.Fatalf("Next call %d failed: %v", i+1, err)
			}
			if batch.Columns[0].Len() != 1 {
				t.Errorf("Batch %d: expected 1 row, got %d", i+1, batch.Columns[0].Len())
			}
		}
	})

	t.Run("Large batch size with fewer rows", func(t *testing.T) {
		csvData := `num
1
2`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		batch, err := proj.Next(1000)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		if batch.Columns[0].Len() != 2 {
			t.Errorf("Expected 2 rows, got %d", batch.Columns[0].Len())
		}
	})

	t.Run("EOF mid-batch breaks correctly", func(t *testing.T) {
		csvData := `id
1
2
3`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		// Request 10 rows, but only 3 exist
		batch, err := proj.Next(10)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		// Should get 3 rows (break on EOF, not error)
		if batch.Columns[0].Len() != 3 {
			t.Errorf("Expected 3 rows when hitting EOF mid-batch, got %d", batch.Columns[0].Len())
		}
	})

	t.Run("Boolean false value handling", func(t *testing.T) {
		csvData := `active
false
true
false`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		batch, err := proj.Next(10)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		boolCol, ok := batch.Columns[0].(*array.Boolean)
		if !ok {
			t.Fatalf("Expected *array.Boolean, got %T", batch.Columns[0])
		}

		// Verify false values are correctly stored
		if boolCol.Value(0) != false {
			t.Error("Expected false at index 0")
		}
		if boolCol.Value(1) != true {
			t.Error("Expected true at index 1")
		}
		if boolCol.Value(2) != false {
			t.Error("Expected false at index 2")
		}
	})

	t.Run("Column ordering matches schema", func(t *testing.T) {
		csvData := `z,y,x
1,2,3
4,5,6`
		reader := strings.NewReader(csvData)
		proj, err := NewProjectCSVLeaf(reader)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		batch, err := proj.Next(10)
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}

		// Verify schema field order
		fields := batch.Schema.Fields()
		if fields[0].Name != "z" || fields[1].Name != "y" || fields[2].Name != "x" {
			t.Error("Schema field order doesn't match CSV header order")
		}

		// Verify data is in correct columns
		zCol := batch.Columns[0].(*array.Int64)
		yCol := batch.Columns[1].(*array.Int64)
		xCol := batch.Columns[2].(*array.Int64)

		if zCol.Value(0) != 1 || yCol.Value(0) != 2 || xCol.Value(0) != 3 {
			t.Error("First row data not in correct column order")
		}
		if zCol.Value(1) != 4 || yCol.Value(1) != 5 || xCol.Value(1) != 6 {
			t.Error("Second row data not in correct column order")
		}
	})
}

// TestIntegrationWithRealFile tests with the actual test file
func TestIntegrationWithRealFile(t *testing.T) {
	t.Run("Real file - multiple batches", func(t *testing.T) {
		v := getTestFile()
		defer func() {
			if err := v.Close(); err != nil {
				t.Fatalf("failed to close: %v", err)
			}
		}()

		proj, err := NewProjectCSVLeaf(v)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		totalRows := 0
		batchCount := 0

		for {
			batch, err := proj.Next(10)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Next failed on batch %d: %v", batchCount+1, err)
			}

			batchCount++
			if len(batch.Columns) > 0 {
				totalRows += batch.Columns[0].Len()
			}

			// Verify all columns have same length
			expectedLen := batch.Columns[0].Len()
			for i, col := range batch.Columns {
				if col.Len() != expectedLen {
					t.Errorf("Batch %d, Column %d: length mismatch, expected %d, got %d",
						batchCount, i, expectedLen, col.Len())
				}
			}
		}

		if batchCount == 0 {
			t.Error("Expected at least one batch from real file")
		}
		if totalRows == 0 {
			t.Error("Expected at least one row from real file")
		}

		t.Logf("Read %d batches with total of %d rows", batchCount, totalRows)
	})

	t.Run("Real file - schema validation", func(t *testing.T) {
		v := getTestFile()
		defer func() {
			if err := v.Close(); err != nil {
				t.Fatalf("failed to close: %v", err)
			}
		}()

		proj, err := NewProjectCSVLeaf(v)
		if err != nil {
			t.Fatalf("NewProjectCSVLeaf failed: %v", err)
		}

		if proj.schema == nil {
			t.Fatal("Schema is nil")
		}

		fields := proj.schema.Fields()
		if len(fields) == 0 {
			t.Error("Schema has no fields")
		}

		// Verify all fields are nullable
		for _, field := range fields {
			if !field.Nullable {
				t.Errorf("Field %s is not nullable", field.Name)
			}
		}

		// Verify colPosition map matches schema
		if len(proj.colPosition) != len(fields) {
			t.Errorf("Column position map size (%d) doesn't match schema field count (%d)",
				len(proj.colPosition), len(fields))
		}

		for i, field := range fields {
			pos, exists := proj.colPosition[field.Name]
			if !exists {
				t.Errorf("Field %s not found in column position map", field.Name)
			}
			if pos != i {
				t.Errorf("Field %s: expected position %d, got %d", field.Name, i, pos)
			}
		}
	})
}
func TestProccessFirstLine(t *testing.T) {
	v := getTestFile()
	p, err := NewProjectCSVLeaf(v)
	if err != nil {
		t.Errorf("Failed to create ProjectCSVLeaf: %v", err)
	}
	defer func() {
		if err := v.Close(); err != nil {
			t.Fatalf("failed to close: %v", err)
		}
	}()
	var builders []array.Builder
	for range len(p.schema.Fields()) {
		builder := array.NewBuilder(memory.DefaultAllocator, &arrow.Date64Type{})
		defer builder.Release()
		builders = append(builders, builder)
	}
	err = p.processRow([]string{"1", "alice", "95.5", "true"}, builders)
	if err == nil {
		t.Errorf("Expected error for empty row, got nil")
	}

}

/*
func TestLargercsvFile(t *testing.T) {
	f1 := getTestFile2()

	project, err := NewProjectCSVLeaf(f1)
	if err != nil {
		t.Fatalf("NewProjectCSVLeaf failed: %v", err)
	}
	defer func() {
		if err := f1.Close(); err != nil {
			t.Fatalf("failed to close: %v", err)
		}
	}()
	for {
		rc, err := project.Next(1024 * 8)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
		fmt.Printf("rc : %v\n", rc.Columns)
	}
}
*/
