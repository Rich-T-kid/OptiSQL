package project

import (
	"fmt"
	"io"
	"opti-sql-go/operators"

	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

var (
	_ = (operators.Operator)(&InMemorySource{})
)

// in memory format just for the ease of testing
// same as other sources, we can use structs/slices here

// thankfully we already covered most of this in record.go
// add a couple utility functions for go types and this should be good to go
var (
	ErrInvalidInMemoryDataType = func(Type any) error {
		return fmt.Errorf("%T is not a supported in memory dataType for InMemoryProjectExec", Type)
	}
)

type InMemorySource struct {
	schema        *arrow.Schema
	columns       []arrow.Array
	pos           uint16
	fieldToColIDx map[string]int
}

func NewInMemoryProjectExec(names []string, columns []any) (*InMemorySource, error) {
	if len(names) != len(columns) {
		return nil, operators.ErrInvalidSchema("number of column names and columns do not match")
	}
	fields := make([]arrow.Field, 0, len(names))
	arrays := make([]arrow.Array, 0, len(names))
	fieldToColIDx := make(map[string]int)
	// parse schema from each of the columns
	for i, col := range columns {
		if !supportedType(col) {
			return nil, operators.ErrInvalidSchema(fmt.Sprintf("unsupported column type for column %s", names[i]))
		}
		field, arr, err := unpackColumn(names[i], col)
		if err != nil {
			return nil, ErrInvalidInMemoryDataType(col)
		}
		fields = append(fields, field)
		arrays = append(arrays, arr)
		fieldToColIDx[field.Name] = i
	}
	return &InMemorySource{
		schema:        arrow.NewSchema(fields, nil),
		columns:       arrays,
		fieldToColIDx: fieldToColIDx,
	}, nil
}
func (ms *InMemorySource) withFields(names ...string) error {

	newSchema, cols, err := ProjectSchemaFilterDown(ms.schema, ms.columns, names...)
	if err != nil {
		return err
	}
	newMap := make(map[string]int)
	for i, f := range newSchema.Fields() {
		newMap[f.Name] = i
	}
	ms.schema = newSchema
	ms.fieldToColIDx = newMap
	ms.columns = cols
	return nil
}
func (ms *InMemorySource) Next(n uint16) (*operators.RecordBatch, error) {
	if len(ms.columns) == 0 || ms.pos >= uint16(ms.columns[0].Len()) {
		return nil, io.EOF // EOF
	}
	var currRows uint16 = 0
	outPutCols := make([]arrow.Array, len(ms.schema.Fields()))

	for i, field := range ms.schema.Fields() {
		col := ms.columns[ms.fieldToColIDx[field.Name]]
		colLen := uint16(col.Len())
		remaining := colLen - ms.pos
		toRead := n
		if remaining < n {
			toRead = remaining
		}
		slice := array.NewSlice(col, int64(ms.pos), int64(ms.pos+toRead))
		outPutCols[i] = slice
		currRows = toRead
	}
	ms.pos += currRows

	return &operators.RecordBatch{
		Schema:   ms.schema,
		Columns:  outPutCols,
		RowCount: uint64(currRows),
	}, nil
}
func (ms *InMemorySource) Close() error {
	for _, c := range ms.columns {
		c.Release()
	}
	return nil
}
func (ms *InMemorySource) Schema() *arrow.Schema {
	return ms.schema
}
func unpackColumn(name string, col any) (arrow.Field, arrow.Array, error) {
	// need to not only build the array; but also need the schema
	var field arrow.Field
	field.Name = name
	field.Nullable = true // default to nullable
	switch colType := col.(type) {
	case []int:
		field.Type = arrow.PrimitiveTypes.Int64
		data := colType
		b := array.NewInt64Builder(memory.DefaultAllocator)
		defer b.Release()
		for _, v := range data {
			b.Append(int64(v))
		}
		return field, b.NewArray(), nil
	case []int8:
		field.Type = arrow.PrimitiveTypes.Int8
		data := colType
		b := array.NewInt8Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(data, nil)
		return field, b.NewArray(), nil
	case []int16:
		field.Type = arrow.PrimitiveTypes.Int16
		data := colType
		b := array.NewInt16Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(data, nil)
		return field, b.NewArray(), nil
	case []int32:
		field.Type = arrow.PrimitiveTypes.Int32
		data := colType
		b := array.NewInt32Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(data, nil)
		return field, b.NewArray(), nil
		// build int32 array
	case []int64:
		field.Type = arrow.PrimitiveTypes.Int64
		data := colType
		b := array.NewInt64Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(data, nil)
		return field, b.NewArray(), nil
	case []uint:
		field.Type = arrow.PrimitiveTypes.Uint64
		data := colType
		b := array.NewUint64Builder(memory.DefaultAllocator)
		defer b.Release()
		for _, v := range data {
			b.Append(uint64(v))
		}
		return field, b.NewArray(), nil
	case []uint8:
		field.Type = arrow.PrimitiveTypes.Uint8
		data := colType
		b := array.NewUint8Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(data, nil)
		return field, b.NewArray(), nil
	case []uint16:
		field.Type = arrow.PrimitiveTypes.Uint16
		data := colType
		b := array.NewUint16Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(data, nil)
		return field, b.NewArray(), nil
	case []uint32:
		field.Type = arrow.PrimitiveTypes.Uint32
		data := colType
		b := array.NewUint32Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(data, nil)
		return field, b.NewArray(), nil
	case []uint64:
		field.Type = arrow.PrimitiveTypes.Uint64
		data := colType
		b := array.NewUint64Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(data, nil)
		return field, b.NewArray(), nil
	case []float32:
		field.Type = arrow.PrimitiveTypes.Float32
		data := colType
		b := array.NewFloat32Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(data, nil)
		return field, b.NewArray(), nil
	case []float64:
		field.Type = arrow.PrimitiveTypes.Float64
		data := colType
		b := array.NewFloat64Builder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(data, nil)
		return field, b.NewArray(), nil
	case []string:
		field.Type = arrow.BinaryTypes.String
		data := colType
		b := array.NewStringBuilder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(data, nil)
		return field, b.NewArray(), nil
	case []bool:
		field.Type = arrow.FixedWidthTypes.Boolean
		data := colType
		b := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer b.Release()
		b.AppendValues(data, nil)
		return field, b.NewArray(), nil

	}
	return arrow.Field{}, nil, fmt.Errorf("unsupported column type for column %s", name)
}
func supportedType(col any) bool {
	switch col.(type) {
	case []int, []int8, []int16, []int32, []int64,
		[]uint, []uint8, []uint16, []uint32, []uint64,
		[]float32, []float64,
		[]string,
		[]bool:
		return true
	default:
		return false
	}
}

// handle keeping only the request columns but make sure the schema and columns are also aligned
// returns error if a column doesnt exist
func ProjectSchemaFilterDown(schema *arrow.Schema, cols []arrow.Array, keepCols ...string) (*arrow.Schema, []arrow.Array, error) {
	if len(keepCols) == 0 {
		return arrow.NewSchema([]arrow.Field{}, nil), nil, ErrEmptyColumnsToProject
	}

	// Build map: columnName -> original index
	fieldIndex := make(map[string]int) // age -> 0
	for i, f := range schema.Fields() {
		fieldIndex[f.Name] = i
	}

	newFields := make([]arrow.Field, 0, len(keepCols))
	newCols := make([]arrow.Array, 0, len(keepCols))

	// Preserve order from keepCols, not schema order
	for _, name := range keepCols {
		idx, exists := fieldIndex[name]
		if !exists {
			return arrow.NewSchema([]arrow.Field{}, nil), []arrow.Array{}, ErrProjectColumnNotFound
		}

		newFields = append(newFields, schema.Field(idx))
		col := cols[idx]
		col.Retain()
		newCols = append(newCols, col)
	}

	newSchema := arrow.NewSchema(newFields, nil)
	return newSchema, newCols, nil
}
