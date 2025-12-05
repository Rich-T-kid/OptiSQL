package Expr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"opti-sql-go/operators"
	"regexp"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

var (
	ErrUnsupportedExpression = func(info string) error {
		return fmt.Errorf("unsupported expression passed to EvalExpression: %s", info)
	}
	ErrCantCompareDifferentTypes = func(leftType, rightType arrow.DataType) error {
		return fmt.Errorf("cannot compare different data types: %s and %s", leftType, rightType)
	}
)

type binaryOperator int

const (
	// arithmetic
	Addition       binaryOperator = 1
	Subtraction    binaryOperator = 2
	Multiplication binaryOperator = 3
	Division       binaryOperator = 4
	// comparison
	Equal              binaryOperator = 6
	NotEqual           binaryOperator = 7
	LessThan           binaryOperator = 8
	LessThanOrEqual    binaryOperator = 9
	GreaterThan        binaryOperator = 10
	GreaterThanOrEqual binaryOperator = 11
	// logical
	And binaryOperator = 12
	Or  binaryOperator = 13
	// RegEx expressions
	Like binaryOperator = 14 // where column_name like "patte%n_with_wi%dcard_"
)

type supportedFunctions int

const (
	Upper supportedFunctions = 1
	Lower supportedFunctions = 2
	Abs   supportedFunctions = 3
	Round supportedFunctions = 4
)

type aggFunctions = int

const (
	Sum   aggFunctions = 1
	Count aggFunctions = 2
	Avg   aggFunctions = 3
	Min   aggFunctions = 4
	Max   aggFunctions = 5
)

var (
	_ = (Expression)(&Alias{})
	_ = (Expression)(&ColumnResolve{})
	_ = (Expression)(&LiteralResolve{})
	_ = (Expression)(&BinaryExpr{})
	_ = (Expression)(&ScalarFunction{})
	_ = (Expression)(&CastExpr{})
)

/*
Eval(expr):

	match expr:
	    Literal(x) -> return x
	    Column(name) -> return array of that column
	    BinaryExpr(left > right) -> eval left, eval right, apply operator
	    ScalarFunction(upper(name)) -> evaluate function
	    Alias(expr, name) -> just a name wrapper
*/
type Expression interface {
	//ExprNode(expr Expr, batch *operators.RecordBatch) (arrow.Array, error)
	// empty method, only for the sake of polymorphism
	ExprNode()
	fmt.Stringer
}

func EvalExpression(expr Expression, batch *operators.RecordBatch) (arrow.Array, error) {
	switch e := expr.(type) {
	case *Alias:
		return EvalAlias(e, batch)
	case *ColumnResolve:
		return EvalColumn(e, batch)
	case *LiteralResolve:
		return EvalLiteral(e, batch)
	case *BinaryExpr:
		return EvalBinary(e, batch)
	case *ScalarFunction:
		return EvalScalarFunction(e, batch)
	case *CastExpr:
		return EvalCast(e, batch)
	case *NullCheckExpr:
		return EvalNullCheckMask(e.Expr, batch)
	default:
		return nil, ErrUnsupportedExpression(expr.String())
	}
}

func ExprDataType(e Expression, inputSchema *arrow.Schema) (arrow.DataType, error) {
	switch ex := e.(type) {

	case *LiteralResolve:
		return ex.Type, nil

	case *ColumnResolve:
		idx := inputSchema.FieldIndices(ex.Name)
		if len(idx) == 0 {
			return nil, fmt.Errorf("exprDataType: unknown column %q", ex.Name)
		}
		return inputSchema.Field(idx[0]).Type, nil
	case *Alias:
		// alias does NOT change type
		return ExprDataType(ex.Expr, inputSchema)

	case *CastExpr:
		return ex.TargetType, nil

	case *BinaryExpr:
		leftType, err := ExprDataType(ex.Left, inputSchema)
		if err != nil {
			return nil, err
		}
		rightType, err := ExprDataType(ex.Right, inputSchema)
		if err != nil {
			return nil, err
		}
		return inferBinaryType(leftType, ex.Op, rightType), nil

	case *ScalarFunction:
		argType, err := ExprDataType(ex.Arguments, inputSchema)
		if err != nil {
			return nil, err
		}
		return inferScalarFunctionType(ex.Function, argType), nil
	case *NullCheckExpr:
		return arrow.FixedWidthTypes.Boolean, nil

	default:
		return nil, ErrUnsupportedExpression(ex.String())
	}
}
func NewExpressions(exprs ...Expression) []Expression {
	return exprs
}

/*
Alias | sql: select col1 as new_name from table_source
updates the column name in the output schema.
*/
type Alias struct {
	Expr Expression
	Name string
}

func NewAlias(expr Expression, name string) *Alias {
	return &Alias{
		Expr: expr,
		Name: name,
	}
}

func EvalAlias(a *Alias, batch *operators.RecordBatch) (arrow.Array, error) {
	return EvalExpression(a.Expr, batch)
}
func (a *Alias) ExprNode() {}
func (a *Alias) String() string {
	return fmt.Sprintf("Alias(%s AS %s)", a.Expr, a.Name)

}

// resolves the arrow array corresponding to name passed in
// sql: select age
type ColumnResolve struct {
	Name string
}

func NewColumnResolve(name string) *ColumnResolve {
	return &ColumnResolve{Name: name}
}

func EvalColumn(c *ColumnResolve, batch *operators.RecordBatch) (arrow.Array, error) {
	// schema and columns are always aligned
	for i, f := range batch.Schema.Fields() {
		if f.Name == c.Name {
			col := batch.Columns[i]
			col.Retain()
			return col, nil
		}
	}
	return nil, fmt.Errorf("column %s not found", c.Name)
}
func (c *ColumnResolve) ExprNode() {}
func (c *ColumnResolve) String() string {
	return fmt.Sprintf("Column(%s)", c.Name)
}

// Evaluates to a column of length = batch-size, filled with this literal.
// sql: select 1
type LiteralResolve struct {
	Type arrow.DataType
	// dont forget to cast the value. so string("hello") not just "hello"
	Value any
}

func NewLiteralResolve(Type arrow.DataType, Value any) *LiteralResolve {
	var castVal any

	switch v := Value.(type) {

	// ------------------------------------------------------
	// INT → cast based on Arrow integer type
	// ------------------------------------------------------
	case int:
		switch Type.ID() {
		case arrow.INT8:
			castVal = int8(v)
		case arrow.INT16:
			castVal = int16(v)
		case arrow.INT32:
			castVal = int32(v)
		case arrow.INT64:
			castVal = int64(v)
		case arrow.UINT8:
			castVal = uint8(v)
		case arrow.UINT16:
			castVal = uint16(v)
		case arrow.UINT32:
			castVal = uint32(v)
		case arrow.UINT64:
			castVal = uint64(v)
		default:
			// not an integer Arrow type → store original
			castVal = v
		}
	case string:
		castVal = string(v)
	case bool:
		castVal = bool(v)
	case float64:
		switch Type.ID() {
		case arrow.FLOAT32:
			castVal = float32(v)
		case arrow.FLOAT64:
			castVal = float64(v)
		}
	default:
		fmt.Printf("%v did not match any case, of type %T\n", v, v)
		castVal = Value
	}
	fmt.Printf("sotred as -> %v\t%v\n", Type, castVal)
	return &LiteralResolve{Type: Type, Value: castVal}
}
func EvalLiteral(l *LiteralResolve, batch *operators.RecordBatch) (arrow.Array, error) {
	n := int(batch.RowCount)

	switch l.Type.ID() {

	// ------------------------------
	// BOOL
	// ------------------------------
	case arrow.BOOL:
		val := l.Value.(bool)
		b := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer b.Release()

		for i := 0; i < n; i++ {
			b.Append(val)
		}
		return b.NewArray(), nil

	// ------------------------------
	// INT / UINT (8/16/32/64)
	// ------------------------------
	case arrow.INT8:
		v := l.Value.(int8)

		b := array.NewInt8Builder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(v)
		}
		return b.NewArray(), nil

	case arrow.UINT8:
		v := l.Value.(uint8)

		b := array.NewUint8Builder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(v)
		}
		return b.NewArray(), nil

	case arrow.INT16:
		v := l.Value.(int16)
		b := array.NewInt16Builder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(v)
		}
		return b.NewArray(), nil

	case arrow.UINT16:
		v := l.Value.(uint16)
		b := array.NewUint16Builder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(v)
		}
		return b.NewArray(), nil

	case arrow.INT32:
		v := l.Value.(int32)
		b := array.NewInt32Builder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(v)
		}
		return b.NewArray(), nil

	case arrow.UINT32:
		v := l.Value.(uint32)
		b := array.NewUint32Builder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(v)
		}
		return b.NewArray(), nil
	case arrow.INT64:
		v := l.Value.(int64)
		b := array.NewInt64Builder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(v)
		}
		return b.NewArray(), nil

	case arrow.UINT64:
		v := l.Value.(uint64)
		b := array.NewUint64Builder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(v)
		}
		return b.NewArray(), nil

	// ------------------------------
	// FLOATS
	// ------------------------------
	case arrow.FLOAT32:
		v := l.Value.(float32)
		b := array.NewFloat32Builder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(v)
		}
		return b.NewArray(), nil

	case arrow.FLOAT64:
		v := l.Value.(float64)
		b := array.NewFloat64Builder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(v)
		}
		return b.NewArray(), nil

	// ------------------------------
	// STRING
	// ------------------------------
	case arrow.STRING:
		v := l.Value.(string)
		b := array.NewStringBuilder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(v)
		}
		return b.NewArray(), nil

	// ------------------------------
	// BINARY
	// ------------------------------
	case arrow.BINARY:
		v := l.Value.([]byte)
		b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.Append(v)
		}
		return b.NewArray(), nil
	// ------------------------------
	// Nulls
	// ------------------------------
	case arrow.NULL:
		b := array.NewNullBuilder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < n; i++ {
			b.AppendNull()
		}
		return b.NewArray(), nil

	default:
		return nil, fmt.Errorf("literal type %s not supported", l.Type)
	}
}

func (l *LiteralResolve) ExprNode() {}
func (l *LiteralResolve) String() string {
	return fmt.Sprintf("Literal(%v)", l.Value)
}

type BinaryExpr struct {
	Left  Expression
	Op    binaryOperator
	Right Expression
}

func NewBinaryExpr(left Expression, op binaryOperator, right Expression) *BinaryExpr {
	return &BinaryExpr{
		Left:  left,
		Op:    op,
		Right: right,
	}
}

func EvalBinary(b *BinaryExpr, batch *operators.RecordBatch) (arrow.Array, error) {
	leftArr, err := EvalExpression(b.Left, batch)
	if err != nil {
		return nil, err
	}
	rightArr, err := EvalExpression(b.Right, batch)
	if err != nil {
		return nil, err
	}
	opt := compute.ArithmeticOptions{}
	switch b.Op {
	// arithmetic
	case Addition:
		datum, err := compute.Add(context.TODO(), opt, compute.NewDatum(leftArr), compute.NewDatum(rightArr))
		if err != nil {
			return nil, err
		}
		return unpackDatum(datum)
	case Subtraction:
		datum, err := compute.Subtract(context.TODO(), opt, compute.NewDatum(leftArr), compute.NewDatum(rightArr))
		if err != nil {
			return nil, err
		}
		return unpackDatum(datum)

	case Multiplication:
		datum, err := compute.Multiply(context.TODO(), opt, compute.NewDatum(leftArr), compute.NewDatum(rightArr))
		if err != nil {
			return nil, err
		}
		return unpackDatum(datum)
	case Division:
		datum, err := compute.Divide(context.TODO(), opt, compute.NewDatum(leftArr), compute.NewDatum(rightArr))
		if err != nil {
			return nil, err
		}
		return unpackDatum(datum)

	// comparisions TODO:
	// These return a boolean array
	case Equal:
		if leftArr.DataType() != rightArr.DataType() {
			return nil, ErrCantCompareDifferentTypes(leftArr.DataType(), rightArr.DataType())
		}
		datum, err := compute.CallFunction(context.Background(), "equal", compute.DefaultFilterOptions(), compute.NewDatum(leftArr), compute.NewDatum(rightArr))
		if err != nil {
			return nil, err
		}
		return unpackDatum(datum)
	case NotEqual:
		if leftArr.DataType() != rightArr.DataType() {
			return nil, ErrCantCompareDifferentTypes(leftArr.DataType(), rightArr.DataType())
		}
		datum, err := compute.CallFunction(context.Background(), "not_equal", compute.DefaultFilterOptions(), compute.NewDatum(leftArr), compute.NewDatum(rightArr))
		if err != nil {
			return nil, err
		}
		return unpackDatum(datum)
	case LessThan:
		if leftArr.DataType() != rightArr.DataType() {
			return nil, ErrCantCompareDifferentTypes(leftArr.DataType(), rightArr.DataType())
		}
		datum, err := compute.CallFunction(context.Background(), "less", compute.DefaultFilterOptions(), compute.NewDatum(leftArr), compute.NewDatum(rightArr))
		if err != nil {
			return nil, err
		}
		return unpackDatum(datum)
	case LessThanOrEqual:
		if leftArr.DataType() != rightArr.DataType() {
			return nil, ErrCantCompareDifferentTypes(leftArr.DataType(), rightArr.DataType())
		}
		datum, err := compute.CallFunction(context.Background(), "less_equal", compute.DefaultFilterOptions(), compute.NewDatum(leftArr), compute.NewDatum(rightArr))
		if err != nil {
			return nil, err
		}
		return unpackDatum(datum)
	case GreaterThan:
		if leftArr.DataType() != rightArr.DataType() {
			return nil, ErrCantCompareDifferentTypes(leftArr.DataType(), rightArr.DataType())
		}
		datum, err := compute.CallFunction(context.Background(), "greater", compute.DefaultFilterOptions(), compute.NewDatum(leftArr), compute.NewDatum(rightArr))
		if err != nil {
			return nil, err
		}
		return unpackDatum(datum)
	case GreaterThanOrEqual:
		if leftArr.DataType() != rightArr.DataType() {
			return nil, ErrCantCompareDifferentTypes(leftArr.DataType(), rightArr.DataType())
		}
		datum, err := compute.CallFunction(context.Background(), "greater_equal", compute.DefaultFilterOptions(), compute.NewDatum(leftArr), compute.NewDatum(rightArr))
		if err != nil {
			return nil, err
		}
		return unpackDatum(datum)
	// logical
	case And:
		if leftArr.DataType() != rightArr.DataType() {
			return nil, ErrCantCompareDifferentTypes(leftArr.DataType(), rightArr.DataType())
		}
		datum, err := compute.CallFunction(context.Background(), "and", compute.DefaultFilterOptions(), compute.NewDatum(leftArr), compute.NewDatum(rightArr))
		if err != nil {
			return nil, err
		}
		return unpackDatum(datum)
	case Or:
		if leftArr.DataType() != rightArr.DataType() {
			return nil, ErrCantCompareDifferentTypes(leftArr.DataType(), rightArr.DataType())
		}
		datum, err := compute.CallFunction(context.Background(), "or", compute.DefaultFilterOptions(), compute.NewDatum(leftArr), compute.NewDatum(rightArr))
		if err != nil {
			return nil, err
		}
		return unpackDatum(datum)
	case Like:
		if leftArr.DataType() != arrow.BinaryTypes.String || rightArr.DataType() != arrow.BinaryTypes.String {
			return nil, errors.New("binary operator Like only works on arrays of strings")
		}
		var compiledRegEx = compileSqlRegEx(rightArr.ValueStr(0))
		filterBuilder := array.NewBooleanBuilder(memory.NewGoAllocator())
		leftStrArray := leftArr.(*array.String)
		for i := 0; i < leftStrArray.Len(); i++ {
			valid := validRegEx(leftStrArray.Value(i), compiledRegEx)
			filterBuilder.Append(valid)
		}
		return filterBuilder.NewArray(), nil

	}
	return nil, fmt.Errorf("binary operator %d not supported", b.Op)
}
func (b *BinaryExpr) ExprNode() {}
func (b *BinaryExpr) String() string {
	return fmt.Sprintf("BinaryExpr(%s %d %s)", b.Left, b.Op, b.Right)
}
func unpackDatum(d compute.Datum) (arrow.Array, error) {
	array, ok := d.(*compute.ArrayDatum)
	if !ok {
		return nil, fmt.Errorf("datum %v is not of type array", d)
	}
	return array.MakeArray(), nil
}

type ScalarFunction struct {
	Function  supportedFunctions
	Arguments Expression // resolve to something you can process IE, literal/coloumn Resolve
}

func NewScalarFunction(function supportedFunctions, Argument Expression) *ScalarFunction {
	return &ScalarFunction{
		Function:  function,
		Arguments: Argument,
	}
}

func EvalScalarFunction(s *ScalarFunction, batch *operators.RecordBatch) (arrow.Array, error) {
	switch s.Function {
	case Upper:
		arr, err := EvalExpression(s.Arguments, batch)
		if err != nil {
			return nil, err
		}
		return upperImpl(arr)

	case Lower:
		arr, err := EvalExpression(s.Arguments, batch)
		if err != nil {
			return nil, err
		}
		return lowerImpl(arr)
	case Abs:
		arr, err := EvalExpression(s.Arguments, batch)
		if err != nil {
			return nil, err
		}
		datum, err := compute.AbsoluteValue(context.TODO(), compute.ArithmeticOptions{}, compute.NewDatum(arr))
		if err != nil {
			return nil, err
		}
		return unpackDatum(datum)
	case Round:
		arr, err := EvalExpression(s.Arguments, batch)
		if err != nil {
			return nil, err
		}
		datum, err := compute.Round(context.TODO(), compute.DefaultRoundOptions, compute.NewDatum(arr))
		if err != nil {
			return nil, err
		}
		return unpackDatum(datum)
	}
	return nil, fmt.Errorf("unsupported scalar function %v", s.Function)
}
func (s *ScalarFunction) ExprNode() {}
func (s *ScalarFunction) String() string {
	return fmt.Sprintf("ScalarFunction(%d, %v)", s.Function, s.Arguments)
}

// If cast succeeds → return the casted value
// If cast fails → throw a runtime error
type CastExpr struct {
	Expr       Expression // can be a Literal or Column (check for datatype when you resolve)
	TargetType arrow.DataType
}

func NewCastExpr(expr Expression, targetType arrow.DataType) *CastExpr {
	return &CastExpr{
		Expr:       expr,
		TargetType: targetType,
	}
}

func EvalCast(c *CastExpr, batch *operators.RecordBatch) (arrow.Array, error) {
	arr, err := EvalExpression(c.Expr, batch)
	if err != nil {
		return nil, err
	}

	// Use Arrow compute kernel to cast
	castOpts := compute.SafeCastOptions(c.TargetType)
	out, err := compute.CastArray(context.TODO(), arr, castOpts)
	if err != nil {
		return nil, fmt.Errorf("cast error: cannot cast %s to %s: %w",
			arr.DataType(), c.TargetType, err)
	}

	return out, nil
}

func (c *CastExpr) ExprNode() {}
func (c *CastExpr) String() string {
	return fmt.Sprintf("Cast(%s AS %s)", c.Expr, c.TargetType)
}

type NullCheckExpr struct {
	Expr Expression
}

func NewNullCheckExpr(expr Expression) *NullCheckExpr {
	return &NullCheckExpr{Expr: expr}
}
func (n *NullCheckExpr) ExprNode() {}
func (n *NullCheckExpr) String() string {
	return fmt.Sprintf("NullCheck(%s)", n.Expr.String())
}
func EvalNullCheckMask(expr Expression, batch *operators.RecordBatch) (arrow.Array, error) {
	// Step 1: Evaluate underlying expression
	arr, err := EvalExpression(expr, batch)
	if err != nil {
		return nil, err
	}

	length := arr.Len()

	// Step 2: Build boolean mask
	builder := array.NewBooleanBuilder(memory.DefaultAllocator)
	builder.Resize(length)

	for i := 0; i < length; i++ {
		builder.Append(!arr.IsNull(i)) // true = not null
	}
	// Step 3: produce final Boolean array
	mask := builder.NewArray()
	builder.Release()
	return mask, nil
}

func upperImpl(arr arrow.Array) (arrow.Array, error) {
	strArr, ok := arr.(*array.String)
	if !ok {
		return nil, fmt.Errorf("upper function only supports string arrays, got %s", arr.DataType())
	}
	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	for i := 0; i < strArr.Len(); i++ {
		if strArr.IsNull(i) {
			b.AppendNull()
		} else {
			b.Append(strings.ToUpper(strArr.Value(i)))
		}
	}
	return b.NewArray(), nil
}
func lowerImpl(arr arrow.Array) (arrow.Array, error) {
	{
		strArr, ok := arr.(*array.String)
		if !ok {
			return nil, fmt.Errorf("lower function only supports string arrays, got %s", arr.DataType())
		}
		b := array.NewStringBuilder(memory.DefaultAllocator)
		defer b.Release()
		for i := 0; i < strArr.Len(); i++ {
			if strArr.IsNull(i) {
				b.AppendNull()
			} else {
				b.Append(strings.ToLower(strArr.Value(i)))
			}
		}
		return b.NewArray(), nil
	}
}
func inferScalarFunctionType(fn supportedFunctions, argType arrow.DataType) arrow.DataType {
	switch fn {

	case Upper, Lower:
		if argType.ID() != arrow.STRING {
			panic("upper/lower only support string types")
		}
		return arrow.BinaryTypes.String

	case Abs, Round:
		return argType // numeric-in numeric-out

	default:
		panic(fmt.Sprintf("unknown scalar function %v", fn))
	}
}

func inferBinaryType(left arrow.DataType, op binaryOperator, right arrow.DataType) arrow.DataType {
	switch op {

	case Addition, Subtraction, Multiplication, Division:
		// numeric → numeric promotion rules
		return numericPromotion(left, right)

	case Equal, NotEqual, LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual:
		return arrow.FixedWidthTypes.Boolean

	case And, Or:
		return arrow.FixedWidthTypes.Boolean

	default:
		panic(fmt.Sprintf("inferBinaryType: unsupported operator %v", op))
	}
}
func numericPromotion(a, b arrow.DataType) arrow.DataType {
	// simplest version: return float64 for any mixed numeric types.
	return arrow.PrimitiveTypes.Float64
}

func compileSqlRegEx(s string) string {
	var buf bytes.Buffer

	// Track anchoring rules
	startsWithWildcard := len(s) > 0 && s[0] == '%'
	endsWithWildcard := len(s) > 0 && s[len(s)-1] == '%'

	// Build body
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '_':
			buf.WriteString(".")
		case '%':
			buf.WriteString(".*")
		default:
			// Escape regex meta chars
			if strings.ContainsRune(`.^$|()[]*+?{}`, rune(s[i])) {
				buf.WriteByte('\\')
			}
			buf.WriteByte(s[i])
		}
	}

	regex := buf.String()

	// Apply anchoring
	if !startsWithWildcard {
		regex = "^" + regex
	}
	if !endsWithWildcard {
		regex = regex + "$"
	}

	return regex
}

func validRegEx(columnValue, regExExpr string) bool {
	ok, _ := regexp.MatchString(regExExpr, columnValue)
	return ok

}
