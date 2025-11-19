package project

import (
	"encoding/csv"
	"fmt"
	"io"
	"opti-sql-go/operators"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

var (
	_ = (operators.Operator)(&CSVSource{})
)

type CSVSource struct {
	r            *csv.Reader
	schema       *arrow.Schema // columns to project as well as types to cast to
	colPosition  map[string]int
	firstDataRow []string
	done         bool // if this is set in Next, we have reached EOF
}

// assume everything is on disk for now
func NewProjectCSVLeaf(source io.Reader) (*CSVSource, error) {
	r := csv.NewReader(source)
	proj := &CSVSource{
		r:           r,
		colPosition: make(map[string]int),
	}
	var err error
	// construct the schema from the header
	proj.schema, err = proj.parseHeader()
	return proj, err
}

func (csvS *CSVSource) Next(n uint16) (*operators.RecordBatch, error) {
	if csvS.done {
		return nil, io.EOF
	}

	// 1. Create builders
	builders := csvS.initBuilders()

	rowsRead := uint16(0)

	// Process stored first row (from parseHeader) ---
	if csvS.firstDataRow != nil && rowsRead < n {
		if err := csvS.processRow(csvS.firstDataRow, builders); err != nil {
			return nil, err
		}
		csvS.firstDataRow = nil // consume it once
		rowsRead++
	}

	//  Stream remaining rows from CSV reader ---
	for rowsRead < n {
		row, err := csvS.r.Read()
		if err == io.EOF {
			if rowsRead == 0 {
				csvS.done = true
				return nil, io.EOF
			}
			break
		}
		if err != nil {
			return nil, err
		}

		// append to builders
		if err := csvS.processRow(row, builders); err != nil {
			return nil, err
		}

		rowsRead++
	}

	//  Freeze into Arrow arrays
	columns := csvS.finalizeBuilders(builders)

	return &operators.RecordBatch{
		Schema:   csvS.schema,
		Columns:  columns,
		RowCount: uint64(rowsRead),
	}, nil
}
func (csvS *CSVSource) Close() error {
	csvS.r = nil
	csvS.done = true
	return nil
}

func (csvS *CSVSource) Schema() *arrow.Schema {
	return csvS.schema
}
func (csvS *CSVSource) initBuilders() []array.Builder {
	fields := csvS.schema.Fields()
	builders := make([]array.Builder, len(fields))

	for i, f := range fields {
		builders[i] = array.NewBuilder(memory.DefaultAllocator, f.Type)
	}

	return builders
}
func (csvS *CSVSource) processRow(
	content []string,
	builders []array.Builder,
) error {
	fields := csvS.schema.Fields()
	for i, f := range fields {
		colIdx := csvS.colPosition[f.Name]
		cell := content[colIdx]

		switch b := builders[i].(type) {

		case *array.Int64Builder:
			if cell == "" || cell == "NULL" {
				b.AppendNull()
			} else {
				v, err := strconv.ParseInt(cell, 10, 64)
				if err != nil {
					b.AppendNull()
				} else {
					b.Append(v)
				}
			}

		case *array.Float64Builder:
			if cell == "" || cell == "NULL" {
				b.AppendNull()
			} else {
				v, err := strconv.ParseFloat(cell, 64)
				if err != nil {
					b.AppendNull()
				} else {
					b.Append(v)
				}
			}

		case *array.StringBuilder:
			if cell == "" || cell == "NULL" {
				b.AppendNull()
			} else {
				b.Append(cell)
			}

		case *array.BooleanBuilder:
			if cell == "" || cell == "NULL" {
				b.AppendNull()
			} else {
				b.Append(cell == "true")
			}

		default:
			return fmt.Errorf("unsupported Arrow type: %s", f.Type)
		}
	}

	return nil
}
func (csvS *CSVSource) finalizeBuilders(builders []array.Builder) []arrow.Array {
	columns := make([]arrow.Array, len(builders))

	for i, b := range builders {
		columns[i] = b.NewArray()
		b.Release()
	}

	return columns
}

// first call to csv.Reader
func (csvS *CSVSource) parseHeader() (*arrow.Schema, error) {
	header, err := csvS.r.Read()
	if err != nil {
		return nil, err
	}
	firstDataRow, err := csvS.r.Read()
	if err != nil {
		return nil, err
	}
	csvS.firstDataRow = firstDataRow
	newFields := make([]arrow.Field, 0, len(header))
	for i, colName := range header {
		sampleValue := firstDataRow[i]
		newFields = append(newFields, arrow.Field{
			Name:     colName,
			Type:     parseDataType(sampleValue),
			Nullable: true,
		})
		csvS.colPosition[colName] = i
	}
	return arrow.NewSchema(newFields, nil), nil
}
func parseDataType(sample string) arrow.DataType {
	sample = strings.TrimSpace(sample)

	// Nulls or empty fields → treat as nullable string in inference
	if sample == "" || strings.EqualFold(sample, "NULL") {
		return arrow.BinaryTypes.String
	}

	// Boolean
	if sample == "true" || sample == "false" {
		return arrow.FixedWidthTypes.Boolean
	}

	// Try int
	if _, err := strconv.Atoi(sample); err == nil {
		return arrow.PrimitiveTypes.Int64
	}

	// Try float
	if _, err := strconv.ParseFloat(sample, 64); err == nil {
		return arrow.PrimitiveTypes.Float64
	}

	// Fallback to string
	return arrow.BinaryTypes.String
}

/*
Integers (int8, int16, int32, int64) - whole numbers like 42, -100
Floating point (float32, float64) - decimal numbers like 3.14, -0.5
Booleans - true/false values (often represented as "true"/"false", "1"/"0", or "yes"/"no")
Strings (text) - any text like "hello", "John Doe"
Nulls
*/
