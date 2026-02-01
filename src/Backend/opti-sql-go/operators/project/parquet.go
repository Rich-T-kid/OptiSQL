package project

import (
	"context"
	"errors"
	"fmt"
	"io"
	"opti-sql-go/config"
	"opti-sql-go/operators"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

var (
	_      = (operators.Operator)(&ParquetSource{})
	Config = config.GetConfig()
)

type ParquetSource struct {
	schema             *arrow.Schema
	projectionPushDown []string // columns to project up
	reader             pqarrow.RecordReader
	bufferedCols       []arrow.Array // internal buffer for excess rows
	bufferedSize       int64         // number of rows currently buffered
	done               bool          // if set to true always return io.EOF
}

func NewParquetSource(r parquet.ReaderAtSeeker) (*ParquetSource, error) {
	allocator := memory.NewGoAllocator()
	filerReader, err := file.NewParquetReader(r)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := filerReader.Close(); err != nil {
			fmt.Printf("warning: failed to close parquet reader: %v\n", err)
		}
	}()

	arrowReader, err := pqarrow.NewFileReader(
		filerReader,
		pqarrow.ArrowReadProperties{Parallel: true, BatchSize: int64(Config.Batch.Size)},
		allocator,
	)
	if err != nil {
		return nil, err
	}
	rdr, err := arrowReader.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		return nil, err
	}

	return &ParquetSource{
		schema:             rdr.Schema(),
		projectionPushDown: []string{},
		reader:             rdr,
		bufferedCols:       make([]arrow.Array, rdr.Schema().NumFields()),
	}, nil

}

// source, columns you want to be push up the tree, any filters
func NewParquetSourcePushDown(r parquet.ReaderAtSeeker, columns []string) (*ParquetSource, error) {
	if len(columns) == 0 {
		return nil, errors.New("no columns were provided for projection push down")
	}
	allocator := memory.NewGoAllocator()
	filerReader, err := file.NewParquetReader(r)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := filerReader.Close(); err != nil {
			fmt.Printf("warning: failed to close parquet reader: %v\n", err)

		}
	}()

	arrowReader, err := pqarrow.NewFileReader(
		filerReader,
		pqarrow.ArrowReadProperties{Parallel: true, BatchSize: int64(Config.Batch.Size)},
		allocator,
	)
	if err != nil {
		return nil, err
	}
	var wantedColumnsIDX []int
	s, _ := arrowReader.Schema()
	for _, col := range columns {
		idx_array := s.FieldIndices(col)
		if len(idx_array) == 0 {
			return nil, errors.New("unknown column passed in to be project push down")
		}
		wantedColumnsIDX = append(wantedColumnsIDX, idx_array...)
	}

	rdr, err := arrowReader.GetRecordReader(context.Background(), wantedColumnsIDX, nil)
	if err != nil {
		return nil, err
	}

	return &ParquetSource{
		schema:             rdr.Schema(),
		projectionPushDown: columns,
		reader:             rdr,
		bufferedCols:       make([]arrow.Array, rdr.Schema().NumFields()),
	}, nil
}

// double check that this return exactly n rows in a column.
// ! buffer in memory if what is read in is too much  > n
func (ps *ParquetSource) Next(n uint16) (*operators.RecordBatch, error) {
	if ps.reader == nil || (ps.done && ps.bufferedSize == 0) {
		return nil, io.EOF
	}

	mem := memory.NewGoAllocator()

	// Read more data if buffer doesn't have enough rows
	for ps.bufferedSize < int64(n) && !ps.done {
		if !ps.reader.Next() {
			ps.done = true
			break
		}

		err := ps.reader.Err()
		if err != nil {
			return nil, err
		}

		record := ps.reader.Record()
		numCols := int(record.NumCols())
		numRows := int(record.NumRows())

		for colIdx := 0; colIdx < numCols; colIdx++ {
			batchCol := record.Column(colIdx)
			existing := ps.bufferedCols[colIdx]

			if existing == nil {
				batchCol.Retain()
				ps.bufferedCols[colIdx] = batchCol
			} else {
				// Concatenate existing + new
				combined, err := array.Concatenate([]arrow.Array{existing, batchCol}, mem)
				if err != nil {
					return nil, err
				}
				existing.Release()
				ps.bufferedCols[colIdx] = combined
			}
		}

		ps.bufferedSize += int64(numRows)
		record.Release()
	}

	// If buffer is empty, return EOF
	if ps.bufferedSize == 0 {
		return nil, io.EOF
	}

	// Emit up to n rows
	toEmit := min(int64(n), ps.bufferedSize)
	out, err := ps.sliceBufferCols(toEmit, mem)
	if err != nil {
		return nil, err
	}

	return &operators.RecordBatch{
		Schema:   ps.schema,
		Columns:  out,
		RowCount: uint64(toEmit),
	}, nil
}

func (ps *ParquetSource) sliceBufferCols(n int64, mem memory.Allocator) ([]arrow.Array, error) {
	out := make([]arrow.Array, len(ps.bufferedCols))

	total := ps.bufferedSize
	limit := n
	if limit > total {
		limit = total
	}

	// For each column: slice out rows to emit and rows to keep
	for i, col := range ps.bufferedCols {
		// emit slice [0:limit]
		sliceOut := array.NewSlice(col, 0, limit)
		out[i] = sliceOut

		// keep remaining slice [limit:total]
		keepSlice := array.NewSlice(col, limit, total)

		// release old buffer column
		col.Release()

		// store updated buffer
		ps.bufferedCols[i] = keepSlice
	}

	// update size
	ps.bufferedSize = total - limit

	return out, nil
}

func (ps *ParquetSource) Close() error {
	ps.reader.Release()
	ps.reader = nil
	return nil
}
func (ps *ParquetSource) Schema() *arrow.Schema {
	return ps.schema
}

func (ps *ParquetSource) Name() string {
	return "Parquet Source"
}

// append arr2 to arr1 so (arr1 + arr2) = arr1-arr2
func CombineArray(a1, a2 arrow.Array) arrow.Array {
	if a1 == nil {
		return a2
	}
	if a2 == nil {
		return a1
	}

	mem := memory.NewGoAllocator()
	dt := a1.DataType()

	switch dt.ID() {

	// -------------------- INT TYPES --------------------
	case arrow.INT8:
		b := array.NewInt8Builder(mem)
		appendInt8(b, a1.(*array.Int8))
		appendInt8(b, a2.(*array.Int8))
		return b.NewArray()

	case arrow.INT16:
		b := array.NewInt16Builder(mem)
		appendInt16(b, a1.(*array.Int16))
		appendInt16(b, a2.(*array.Int16))
		return b.NewArray()

	case arrow.INT32:
		b := array.NewInt32Builder(mem)
		appendInt32(b, a1.(*array.Int32))
		appendInt32(b, a2.(*array.Int32))
		return b.NewArray()

	case arrow.INT64:
		b := array.NewInt64Builder(mem)
		appendInt64(b, a1.(*array.Int64))
		appendInt64(b, a2.(*array.Int64))
		return b.NewArray()

	// -------------------- UINT TYPES --------------------
	case arrow.UINT8:
		b := array.NewUint8Builder(mem)
		appendUint8(b, a1.(*array.Uint8))
		appendUint8(b, a2.(*array.Uint8))
		return b.NewArray()

	case arrow.UINT16:
		b := array.NewUint16Builder(mem)
		appendUint16(b, a1.(*array.Uint16))
		appendUint16(b, a2.(*array.Uint16))
		return b.NewArray()

	case arrow.UINT32:
		b := array.NewUint32Builder(mem)
		appendUint32(b, a1.(*array.Uint32))
		appendUint32(b, a2.(*array.Uint32))
		return b.NewArray()

	case arrow.UINT64:
		b := array.NewUint64Builder(mem)
		appendUint64(b, a1.(*array.Uint64))
		appendUint64(b, a2.(*array.Uint64))
		return b.NewArray()

	// -------------------- FLOAT TYPES --------------------
	case arrow.FLOAT32:
		b := array.NewFloat32Builder(mem)
		appendFloat32(b, a1.(*array.Float32))
		appendFloat32(b, a2.(*array.Float32))
		return b.NewArray()

	case arrow.FLOAT64:
		b := array.NewFloat64Builder(mem)
		appendFloat64(b, a1.(*array.Float64))
		appendFloat64(b, a2.(*array.Float64))
		return b.NewArray()

	// -------------------- BOOLEAN --------------------
	case arrow.BOOL:
		b := array.NewBooleanBuilder(mem)
		appendBool(b, a1.(*array.Boolean))
		appendBool(b, a2.(*array.Boolean))
		return b.NewArray()

	// -------------------- STRING TYPES --------------------
	case arrow.STRING:
		b := array.NewStringBuilder(mem)
		appendString(b, a1.(*array.String))
		appendString(b, a2.(*array.String))
		return b.NewArray()

	case arrow.LARGE_STRING:
		b := array.NewLargeStringBuilder(mem)
		appendLargeString(b, a1.(*array.LargeString))
		appendLargeString(b, a2.(*array.LargeString))
		return b.NewArray()

	// -------------------- BINARY TYPES --------------------
	case arrow.BINARY:
		b := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		appendBinary(b, a1.(*array.Binary))
		appendBinary(b, a2.(*array.Binary))
		return b.NewArray()
		// -------------------- TIMESTAMP[TZ] --------------------
	case arrow.TIMESTAMP:
		tsType := dt.(*arrow.TimestampType) // keeps unit + timezone

		b := array.NewTimestampBuilder(mem, tsType)
		arr1 := a1.(*array.Timestamp)
		arr2 := a2.(*array.Timestamp)

		// append arr1
		for i := 0; i < arr1.Len(); i++ {
			if arr1.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(arr1.Value(i))
			}
		}

		// append arr2
		for i := 0; i < arr2.Len(); i++ {
			if arr2.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(arr2.Value(i))
			}
		}

		return b.NewArray()

	default:
		panic(fmt.Sprintf("unsupported datatype in CombineArray: %v", dt))
	}
}

func appendInt8(b *array.Int8Builder, c *array.Int8) {
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(c.Value(i))
	}
}

func appendInt16(b *array.Int16Builder, c *array.Int16) {
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(c.Value(i))
	}
}

func appendInt32(b *array.Int32Builder, c *array.Int32) {
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(c.Value(i))
	}
}

func appendInt64(b *array.Int64Builder, c *array.Int64) {
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(c.Value(i))
	}
}

func appendUint8(b *array.Uint8Builder, c *array.Uint8) {
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(c.Value(i))
	}
}

func appendUint16(b *array.Uint16Builder, c *array.Uint16) {
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(c.Value(i))
	}
}

func appendUint32(b *array.Uint32Builder, c *array.Uint32) {
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(c.Value(i))
	}
}

func appendUint64(b *array.Uint64Builder, c *array.Uint64) {
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(c.Value(i))
	}
}

func appendFloat32(b *array.Float32Builder, c *array.Float32) {
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(c.Value(i))
	}
}

func appendFloat64(b *array.Float64Builder, c *array.Float64) {
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(c.Value(i))
	}
}

func appendBool(b *array.BooleanBuilder, c *array.Boolean) {
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(c.Value(i))
	}
}

func appendString(b *array.StringBuilder, c *array.String) {
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(c.Value(i))
	}
}

func appendLargeString(b *array.LargeStringBuilder, c *array.LargeString) {
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(c.Value(i))
	}
}

func appendBinary(b *array.BinaryBuilder, c *array.Binary) {
	for i := 0; i < c.Len(); i++ {
		if c.IsNull(i) {
			b.AppendNull()
			continue
		}
		b.Append(c.Value(i))
	}
}
