package operators

import (
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

var (
	ErrInvalidSchema = func(info string) error {
		return fmt.Errorf("invalid schema was provided. context: %s", info)
	}
)

type Operator interface {
	Next(uint16) (*RecordBatch, error)
	Schema() *arrow.Schema
	// Call Operator.Close() after Next returns an io.EOF to clean up resources
	Close() error
}
type RecordBatch struct {
	Schema   *arrow.Schema
	Columns  []arrow.Array
	RowCount uint64 // TODO: update to actually use this, in all operators
}

type SchemaBuilder struct {
	fields []arrow.Field
}

type RecordBatchBuilder struct {
	SchemaBuilder *SchemaBuilder
}

func NewRecordBatchBuilder() *RecordBatchBuilder {
	return &RecordBatchBuilder{
		SchemaBuilder: &SchemaBuilder{
			fields: make([]arrow.Field, 0, 10),
		},
	}
}

func (sb *SchemaBuilder) WithField(name string, dtype arrow.DataType, nullable bool) *SchemaBuilder {
	sb.fields = append(sb.fields, arrow.Field{
		Name:     name,
		Type:     dtype,
		Nullable: nullable,
	})
	return sb
}
func (sb *SchemaBuilder) WithoutField(names ...string) *SchemaBuilder {
	nameSet := make(map[string]struct{}, len(names))
	for _, n := range names {
		nameSet[n] = struct{}{}
	}

	newFields := make([]arrow.Field, 0, len(sb.fields))
	for _, field := range sb.fields {
		_, found := nameSet[field.Name]
		if !found {
			newFields = append(newFields, field)
		}
	}
	sb.fields = newFields
	return sb

}

func (sb *SchemaBuilder) Build() *arrow.Schema {
	return arrow.NewSchema(sb.fields, nil)
}
func (rbb *RecordBatchBuilder) Schema() *arrow.Schema {
	return arrow.NewSchema(rbb.SchemaBuilder.fields, nil)
}

// schema is always right in case of type mismatches
func (rbb *RecordBatchBuilder) validate(schema *arrow.Schema, columns []arrow.Array) error {
	if len(schema.Fields()) != len(columns) {
		return ErrInvalidSchema("schema fields and column count do not match")
	}
	// make sure that the array data types line up with whats expected of the schema
	// Ensure array data types align with schema expectations.
	var errors []string
	for i := 0; i < len(columns); i++ {
		field := schema.Field(i)
		colType := columns[i].DataType()

		if !arrow.TypeEqual(colType, field.Type) {
			errors = append(errors,
				fmt.Sprintf("Type mismatch at position %d: column '%s' has type '%s', but schema expects '%s'.",
					i, field.Name, colType, field.Type))
		}
	}
	if len(errors) > 0 {
		return ErrInvalidSchema(strings.Join(errors, " "))
	}
	return nil
}
func (rbb *RecordBatchBuilder) NewRecordBatch(schema *arrow.Schema, columns []arrow.Array) (*RecordBatch, error) {
	if err := rbb.validate(schema, columns); err != nil {
		return nil, err
	}
	return &RecordBatch{
		Schema:  schema,
		Columns: columns,
	}, nil
}
func (rb *RecordBatch) DeepEqual(other *RecordBatch) bool {
	if !rb.Schema.Equal(other.Schema) {
		return false
	}
	if len(rb.Columns) != len(other.Columns) {
		return false
	}
	for i := 0; i < len(rb.Columns); i++ {
		if !array.Equal(rb.Columns[i], other.Columns[i]) {
			return false
		}
	}
	return true
}
func (rbb *RecordBatchBuilder) GenIntArray(values ...int) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewInt32Builder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(int32(v))
	}
	return builder.NewArray()
}

func (rbb *RecordBatchBuilder) GenFloatArray(values ...float64) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewFloat64Builder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewArray()
}

func (rbb *RecordBatchBuilder) GenStringArray(values ...string) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewStringBuilder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewArray()
}

func (rbb *RecordBatchBuilder) GenBoolArray(values ...bool) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewBooleanBuilder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewArray()
}

// GenInt8Array generates an Int8 array
func (rbb *RecordBatchBuilder) GenInt8Array(values ...int8) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewInt8Builder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewArray()
}

// GenInt16Array generates an Int16 array
func (rbb *RecordBatchBuilder) GenInt16Array(values ...int16) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewInt16Builder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewArray()
}

// GenInt64Array generates an Int64 array
func (rbb *RecordBatchBuilder) GenInt64Array(values ...int64) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewInt64Builder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewArray()
}

// GenUint8Array generates a Uint8 array
func (rbb *RecordBatchBuilder) GenUint8Array(values ...uint8) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewUint8Builder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewArray()
}

// GenUint16Array generates a Uint16 array
func (rbb *RecordBatchBuilder) GenUint16Array(values ...uint16) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewUint16Builder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewArray()
}

// GenUint32Array generates a Uint32 array
func (rbb *RecordBatchBuilder) GenUint32Array(values ...uint32) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewUint32Builder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewArray()
}

// GenUint64Array generates a Uint64 array
func (rbb *RecordBatchBuilder) GenUint64Array(values ...uint64) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewUint64Builder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewArray()
}

// GenFloat32Array generates a Float32 array
func (rbb *RecordBatchBuilder) GenFloat32Array(values ...float32) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewFloat32Builder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewArray()
}

// GenBinaryArray generates a Binary array
func (rbb *RecordBatchBuilder) GenBinaryArray(values ...[]byte) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewArray()
}

// GenLargeStringArray generates a LargeString array
func (rbb *RecordBatchBuilder) GenLargeStringArray(values ...string) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewLargeStringBuilder(mem)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewArray()
}

// GenLargeBinaryArray generates a LargeBinary array
func (rbb *RecordBatchBuilder) GenLargeBinaryArray(values ...[]byte) arrow.Array {
	mem := memory.NewGoAllocator()
	builder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
	defer builder.Release()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.NewArray()
}
