package substrait

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"opti-sql-go/Expr"
	"opti-sql-go/operators"
	"opti-sql-go/operators/aggr"
	"opti-sql-go/operators/filter"
	"opti-sql-go/operators/join"
	"opti-sql-go/operators/project"
	"os"
	"reflect"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
)

var (
	ErrInvalidSubstraitPlan = func(e error) error {
		return fmt.Errorf("invalid JSON from frontend: %s", e.Error())
	}
	ErrMalformedEmitBody = fmt.Errorf("malformed logical plan: multiple root operators found, expected exactly one")

	ErrMissingEmitOperator = fmt.Errorf("malformed logical plan: missing 'Emit' operator")

	ErrInvalidEmitChildren = fmt.Errorf("malformed logical plan: 'Emit' input must be a key to a JSON object")

	ErrInvalidOperator = func(operator string) error {
		return fmt.Errorf("invalid operator '%s': cannot be called directly before 'Emit'", operator)
	}
	ErrBuildTreeFailed = func(operator string, context string) error {
		return fmt.Errorf("failed to build operator tree for '%s': %s", operator, context)
	}
)

type jsonOBJ = map[string]interface{}

/*
sql: select a, b from table1 order by a
"Emit": {
  "Sort": {
    "input": {
      "Project": {
        "input": {
          "Source": "s3://bucket/data.csv"
        },
        "columns": ["a", "b"],
        "alias": ["", ""]
      }
    },
    "by": [{ "column": "a" }]
  }
}
*/
// represents outer layer of substrait plan
type Emiter struct {
	emitOperator operators.Operator
}
type planMetaData struct {
	id            string
	localFileName string // check if empty before deleting the file
}

func NewPlanMetaData(id string) *planMetaData {
	return &planMetaData{id: id}

}

// first turn into json. The plan should fit into ram to consume it all
func consumePlan(r io.Reader, p *planMetaData) (*Emiter, error) {

	contents, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	inMemoryRepr := make(jsonOBJ)
	err = json.Unmarshal(contents, &inMemoryRepr)
	if err != nil {
		return nil, ErrInvalidSubstraitPlan(err)
	}
	if len(inMemoryRepr) != 1 {
		return nil, ErrMalformedEmitBody
	}
	_, exist := inMemoryRepr["Emit"] // TODO! standerdize the spelling and casing of this or else everythign else will break
	if !exist {
		return nil, ErrMissingEmitOperator
	}
	tree, ok := inMemoryRepr["Emit"].(map[string]any)
	if !ok {
		return nil, ErrInvalidEmitChildren
	}
	return buildTree(tree, p)
}

func buildTree(m jsonOBJ, plan *planMetaData) (*Emiter, error) {
	//key=Operator , value=arguments to that operator

	// the tree needs to be built from the bottom up. Recurse all the way down until you reach a leaf node || key == "Source"
	const operator = "Operator"

	if err := containsFields([]string{operator}, m); err != nil {
		return nil, err
	}
	if err := correctFieldTypes([]string{operator}, []string{"string"}, m); err != nil {
		return nil, err
	}

	operatorNode := m[operator].(string)
	body := m[operatorNode].(map[string]any)
	var op operators.Operator
	switch strings.ToLower(operatorNode) {
	case "filter":
		filterOP, err := parseFilter(body, plan)
		if err != nil {
			return nil, ErrBuildTreeFailed("filter", err.Error())
		}
		op = filterOP
		return &Emiter{op}, nil
	case "project":
		projectOP, err := parseProject(body, plan)
		if err != nil {
			return nil, ErrBuildTreeFailed("project", err.Error())
		}
		op = projectOP
		return &Emiter{op}, nil
	case "sort":
		sortOP, err := parseSort(body, plan)
		if err != nil {
			return nil, ErrBuildTreeFailed("sort", err.Error())
		}
		op = sortOP
		return &Emiter{op}, nil

	case "distinct":
		distinctOP, err := parseDistinct(body, plan)
		if err != nil {
			return nil, ErrBuildTreeFailed("distinct", err.Error())
		}
		op = distinctOP
		return &Emiter{op}, nil
	case "limit":
		limitOP, err := parseLimit(body, plan)
		if err != nil {
			return nil, ErrBuildTreeFailed("limit", err.Error())
		}
		op = limitOP
		return &Emiter{op}, nil
	case "aggregate":
		aggrOP, err := parseSingleAggr(body, plan)
		if err != nil {
			return nil, ErrBuildTreeFailed("single-aggr", err.Error())
		}
		op = aggrOP
		return &Emiter{op}, nil
	case "groupby":
		groupByOP, err := parseGroupBy(body, plan)
		if err != nil {
			return nil, ErrBuildTreeFailed("group-by", err.Error())
		}
		op = groupByOP
		return &Emiter{op}, err

	case "join":
		joinOP, err := parseJoin(body, plan)
		if err != nil {
			return nil, ErrBuildTreeFailed("join", err.Error())
		}
		op = joinOP
		return &Emiter{op}, err

	case "source", "expression": // invalid branch
		//(1) Source:cannot directy return from source
		//(2) expressions: cannot directy return expressions, need to call project on top
		return nil, ErrInvalidOperator(operatorNode)
	}
	return nil, ErrBuildTreeFailed("unknown", "no valid operator found in logical plan")
}
func parseSource(sourceOBJ jsonOBJ, plan *planMetaData) (operators.Operator, error) {
	fmt.Printf("sourceOBJ:\t%v\n", sourceOBJ)
	fields := []string{"file-name", "local"}
	err := containsFields(fields, sourceOBJ)
	if err != nil {
		return nil, err
	}
	err = correctFieldTypes(fields, []string{"string", "boolean"}, sourceOBJ)
	if err != nil {
		return nil, err
	}
	name := sourceOBJ["file-name"].(string)
	pieces := strings.Split(name, ".")
	if len(pieces) < 1 {
		return nil, fmt.Errorf("invalid file name used as source, must end in .csv or .parquet")
	}
	var kind string
	switch strings.ToLower(pieces[len(pieces)-1]) {
	case "csv":
		kind = "csv"
	case "parquet":
		kind = "parquet"
	default:
		return nil, fmt.Errorf("invalid file mime was used in source operator")
	}
	local := sourceOBJ["local"].(bool)
	ntwResource, err := project.NewStreamReader(name)
	if err != nil {
		return nil, err
	}
	if !local && kind == "parquet" {
		parquetRootNode, err := project.NewParquetSource(ntwResource)
		if err != nil {
			return nil, err
		}
		return parquetRootNode, nil
	}
	localFile, err := ntwResource.DownloadLocally(plan.id)
	if err != nil {
		return nil, err
	}
	curDir, _ := os.Getwd()
	plan.localFileName = fmt.Sprintf("%s/%s", curDir, localFile.Name())
	switch kind {
	case "csv":
		csvRootNode, err := project.NewProjectCSVLeaf(localFile)
		if err != nil {
			return nil, err
		}
		return csvRootNode, nil
	case "parquet":
		parquetRootNode, err := project.NewParquetSource(localFile)
		if err != nil {
			return nil, err
		}
		return parquetRootNode, nil
	}
	return nil, nil

}
func parseFilter(filterOBJ jsonOBJ, plan *planMetaData) (*filter.FilterExec, error) {
	fields := []string{"input", "expression"}
	err := containsFields(fields, filterOBJ)
	if err != nil {
		return nil, err
	}
	err = correctFieldTypes(fields, []string{"object", "object"}, filterOBJ)
	if err != nil {
		return nil, err
	}
	exprsVal, ok := filterOBJ["expression"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expression field has invalid type, expected map[string]any")
	}
	expression, err := parseExpression(exprsVal)
	if err != nil {
		return nil, err
	}
	var validExpr func(e Expr.Expression) bool // only here so we can call validExpr recusivly
	validExpr = func(e Expr.Expression) bool {
		be, ok := e.(*Expr.BinaryExpr)
		if !ok {
			return false
		}
		switch be.Op {
		case Expr.Equal,
			Expr.NotEqual,
			Expr.LessThan,
			Expr.LessThanOrEqual,
			Expr.GreaterThan,
			Expr.GreaterThanOrEqual:
			return true
		case Expr.And, Expr.Or:
			return validExpr(be.Left) && validExpr(be.Right)
		default:
			return false
		}
	}
	if !validExpr(expression) {
		return nil, fmt.Errorf("%s is not a valid filter/having expression, must evaluate to boolean mask", expression)
	}
	fmt.Printf("input:\t%v\n", filterOBJ["input"])
	input, err := resolveInput(filterOBJ["input"].(map[string]any), plan)
	if err != nil {
		return nil, err
	}
	print(1)
	return filter.NewFilterExec(input, expression)
}
func parseProject(projectOBJ jsonOBJ, plan *planMetaData) (*project.ProjectExec, error) {
	fields := []string{"input", "expressions"}
	err := containsFields(fields, projectOBJ)
	if err != nil {
		return nil, err
	}
	err = correctFieldTypes(fields, []string{"object", "array"}, projectOBJ)
	if err != nil {
		return nil, err
	}
	var expres []Expr.Expression
	exprsVal, ok := projectOBJ["expressions"].([]map[string]any)
	if !ok {
		return nil, fmt.Errorf("expressions field has invalid type, expected []map[string]any")
	}

	for i := range exprsVal {
		expr := exprsVal[i]
		e, err := parseExpression(expr)
		if err != nil {
			return nil, err
		}
		expres = append(expres, e)
	}
	if len(expres) == 0 {
		return nil, fmt.Errorf("project operator needs at least one expressions")
	}
	sourceInput, err := resolveInput(projectOBJ["input"].(map[string]any), plan)
	if err != nil {
		return nil, err
	}
	return project.NewProjectExec(sourceInput, expres)
}
func parseSort(sortOBJ jsonOBJ, plan *planMetaData) (*aggr.SortExec, error) {
	fields := []string{"input", "by"}
	err := containsFields(fields, sortOBJ)
	if err != nil {
		return nil, err
	}
	err = correctFieldTypes(fields, []string{"object", "array"}, sortOBJ)
	if err != nil {
		return nil, err
	}
	parseBy := func(obj []map[string]any) ([]aggr.SortKey, error) {
		var outputKeys []aggr.SortKey
		for _, byexpr := range obj {
			byFields := []string{"expr", "asc"}
			err := containsFields(byFields, byexpr)
			if err != nil {
				return nil, err
			}
			err = correctFieldTypes(byFields, []string{"object", "boolean"}, byexpr)
			if err != nil {
				return nil, err
			}
			expr, err := parseExpression(byexpr["expr"].(map[string]any))
			if err != nil {
				return nil, err
			}
			asc := byexpr["asc"].(bool)
			outputKeys = append(outputKeys, aggr.SortKey{
				Expr:      expr,
				Ascending: asc,
			})

		}
		return outputKeys, nil
	}
	input, err := resolveInput(sortOBJ["input"].(map[string]any), plan)
	if err != nil {
		return nil, err
	}
	byField, ok := sortOBJ["by"].([]map[string]any)
	if !ok {
		return nil, fmt.Errorf("Sort::by field is malformed, should be an array of objects")
	}
	sortKeys, err := parseBy(byField)
	if err != nil {
		return nil, err
	}
	if len(sortKeys) < 1 {
		return nil, fmt.Errorf("sort keys must be present for Sort operator")
	}
	return aggr.NewSortExec(input, sortKeys)
}
func parseDistinct(distinctOBJ jsonOBJ, plan *planMetaData) (*filter.DistinctExec, error) {
	fields := []string{"input", "expressions"}
	err := containsFields(fields, distinctOBJ)
	if err != nil {
		return nil, err
	}
	err = correctFieldTypes(fields, []string{"object", "array"}, distinctOBJ)
	if err != nil {
		return nil, err
	}
	var expres []Expr.Expression
	exprsVal, ok := distinctOBJ["expressions"].([]map[string]any)
	if !ok {
		return nil, fmt.Errorf("expressions field has invalid type, expected []map[string]any")
	}

	for i := range exprsVal {
		expr := exprsVal[i]
		e, err := parseExpression(expr)
		if err != nil {
			return nil, err
		}
		expres = append(expres, e)
	}
	if len(expres) == 0 {
		return nil, fmt.Errorf("distinct operator needs at least one expressions")
	}
	sourceInput, err := resolveInput(distinctOBJ["input"].(map[string]any), plan)
	if err != nil {
		return nil, err
	}
	return filter.NewDistinctExec(sourceInput, expres)
}

func parseLimit(limitOBJ jsonOBJ, plan *planMetaData) (*filter.LimitExec, error) {
	fields := []string{"input", "limit"}
	err := containsFields(fields, limitOBJ)
	if err != nil {
		return nil, err
	}
	err = correctFieldTypes(fields, []string{"object", "int"}, limitOBJ)
	if err != nil {
		return nil, err
	}
	limit, ok := limitOBJ["limit"].(int)
	if !ok {
		return nil, fmt.Errorf("limit field is not the correct type")
	}
	// must be a valid uint16 value 1-2^16
	if limit <= 0 || limit > math.MaxUint16 {
		return nil, fmt.Errorf("limit field cannot be less than 1 or greater than %v, but %v was passed in", math.MaxUint16, limit)
	}
	sourceInput, err := resolveInput(limitOBJ["input"].(map[string]any), plan)
	if err != nil {
		return nil, err
	}

	return filter.NewLimitExec(sourceInput, uint16(limit))
}

func parseSingleAggr(aggrOBJ jsonOBJ, plan *planMetaData) (*aggr.AggrExec, error) {
	fields := []string{"input", "aggrs"}
	err := containsFields(fields, aggrOBJ)
	if err != nil {
		return nil, err
	}
	err = correctFieldTypes(fields, []string{"object", "array"}, aggrOBJ)
	if err != nil {
		return nil, err
	}

	input, err := resolveInput(aggrOBJ["input"].(map[string]any), plan)
	if err != nil {
		return nil, err
	}
	globalAggrs, err := generateAggrs(aggrOBJ["aggrs"].([]map[string]any))
	if err != nil {
		return nil, err
	}
	if len(globalAggrs) < 1 {
		return nil, fmt.Errorf("there must be atleast one aggregation")
	}
	return aggr.NewGlobalAggrExec(input, globalAggrs)
}
func parseGroupBy(groupbyOBJ jsonOBJ, plan *planMetaData) (*aggr.GroupByExec, error) {
	fields := []string{"input", "group_by", "aggrs"}
	err := containsFields(fields, groupbyOBJ)
	if err != nil {
		return nil, err
	}
	err = correctFieldTypes(fields, []string{"object", "array", "array"}, groupbyOBJ)
	if err != nil {
		return nil, err
	}
	input, err := resolveInput(groupbyOBJ["input"].(map[string]any), plan)
	if err != nil {
		return nil, err
	}
	var groupByStatments []Expr.Expression
	group_by, ok := groupbyOBJ["group_by"].([]map[string]any) // array of expressions
	if !ok {
		return nil, fmt.Errorf("group by statments are malformed, should be an array of expressions")
	}
	for _, gb := range group_by {
		e, err := parseExpression(gb)
		if err != nil {
			return nil, err
		}
		groupByStatments = append(groupByStatments, e)
	}
	rawAggrs, ok := groupbyOBJ["aggrs"].([]map[string]any)
	if !ok {
		return nil, fmt.Errorf("aggrs malformed, should be an array of aggregations")
	}
	aggrs, err := generateAggrs(rawAggrs)
	if err != nil {
		return nil, err
	}
	if len(groupByStatments) == 0 {
		return nil, fmt.Errorf("invalid GROUP BY: must have at least one group_by key")
	}
	if len(aggrs) == 0 {
		return nil, fmt.Errorf("invalid GROUP BY: must have at least one aggregation")
	}
	return aggr.NewGroupByExec(input, aggrs, groupByStatments)
}
func parseJoin(joinOBJ jsonOBJ, plan *planMetaData) (*join.HashJoinExec, error) {
	fields := []string{"left", "right", "join_type", "on"}
	err := containsFields(fields, joinOBJ)
	if err != nil {
		return nil, err
	}
	err = correctFieldTypes(fields, []string{"object", "object", "string", "array"}, joinOBJ)
	if err != nil {
		return nil, err
	}
	leftObj, ok := joinOBJ["left"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("malformed join body, `left` field must be an operator/object")
	}
	left, err := resolveInput(leftObj, plan)
	if err != nil {
		return nil, err
	}
	rightObj, ok := joinOBJ["right"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("malformed join body ,`right` field must be an operator/object")
	}
	right, err := resolveInput(rightObj, plan)
	if err != nil {
		return nil, err
	}
	joinType := strings.ToLower(joinOBJ["join_type"].(string))
	if joinType != "inner" {
		return nil, fmt.Errorf("invalid join type provided %s only inner is supported", joinType)
	}
	clauseParer := func(clause []map[string]any) (join.JoinClause, error) {
		var jc join.JoinClause
		for _, c := range clause {
			f := []string{"left", "right"}
			err := containsFields(f, c)
			if err != nil {
				return jc, err
			}
			err = correctFieldTypes(f, []string{"object", "object"}, c)
			if err != nil {
				return jc, err
			}
			leftExpr, err := parseExpression(c["left"].(map[string]any))
			if err != nil {
				return jc, err
			}
			rightExpr, err := parseExpression(c["right"].(map[string]any))
			if err != nil {
				return jc, err
			}
			jc.LeftS = append(jc.LeftS, leftExpr)
			jc.RightS = append(jc.RightS, rightExpr)
		}
		if len(jc.LeftS) < 1 {
			return jc, fmt.Errorf("join clause cannot be empyy")
		}
		return jc, nil
	}
	jc, err := clauseParer(joinOBJ["on"].([]map[string]any))
	if err != nil {
		return nil, err
	}

	return join.NewHashJoinExec(left, right, jc, join.InnerJoin, nil)
}

// carbon clone of
func parseHaving(havingOBJ jsonOBJ, plan *planMetaData) (operators.Operator, error) {
	return parseFilter(havingOBJ, plan)
}

// expressions need to be handled in a special way since they contain serveral keys
func parseExpression(m jsonOBJ) (Expr.Expression, error) {
	// grab tje expr_type and then parse based on that
	err := containsFields([]string{"expr_type"}, m)
	if err != nil {
		return nil, fmt.Errorf("malformed expression body. Doesnt contain expr_type field")
	}
	switch m["expr_type"].(string) {
	case "ColumnResolve":
		neededFields := []string{"name"}
		fieldTypes := []string{"string"}
		err := containsFields(neededFields, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		err = correctFieldTypes(neededFields, fieldTypes, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		if m["name"] == "" {
			return nil, fmt.Errorf("column resolve name cannot be empty")
		}
		cr := Expr.NewColumnResolve(m["name"].(string))
		return cr, nil
	case "LiteralResolve":
		neededFields := []string{"value", "lit_type"}
		err := containsFields(neededFields, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		fieldTypes := []string{m["lit_type"].(string), "string"}
		err = correctFieldTypes(neededFields, fieldTypes, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body (Types): %v", err)
		}
		var value any
		var arrowType arrow.DataType
		switch m["lit_type"].(string) {
		case "int":
			arrowType = arrow.PrimitiveTypes.Int64
			v, _ := m["value"].(int)
			value = int64(v)
		case "string":
			arrowType = arrow.BinaryTypes.String
			v, _ := m["value"].(string)
			value = string(v)
		case "boolean":
			arrowType = arrow.FixedWidthTypes.Boolean
			v, _ := m["value"].(bool)
			value = bool(v)
		case "float64":
			arrowType = arrow.PrimitiveTypes.Float64
			v, _ := m["value"].(float64)
			value = float64(v)
		default:
			return nil, fmt.Errorf("invalid Literal Type was passed to Literal Resolve")
		}
		lr := Expr.NewLiteralResolve(arrowType, value)
		return lr, nil
	case "BinaryExpr":
		neededFields := []string{"op", "left", "right"}
		fieldTypes := []string{"string", "object", "object"}
		err := containsFields(neededFields, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		err = correctFieldTypes(neededFields, fieldTypes, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		left, err := parseExpression(m["left"].(map[string]any))
		if err != nil {
			return nil, err
		}
		right, err := parseExpression(m["right"].(map[string]any))
		if err != nil {
			return nil, err
		}
		op := m["op"].(string)
		operator, err := validBinaryOp(op)
		if err != nil {
			return nil, err
		}
		binaryExpression := Expr.NewBinaryExpr(left, operator, right)
		return binaryExpression, nil
	case "ScalarFunction":
		neededFields := []string{"func", "expr"}
		fieldTypes := []string{"string", "object"}
		err := containsFields(neededFields, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		err = correctFieldTypes(neededFields, fieldTypes, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		function := m["func"].(string)
		fn := Expr.SupportedFunctions(-1)
		switch function {
		case "Upper", "Lower", "Abs", "Round":
			fn = Expr.FnToScalarFunction(function)

		}
		if fn == Expr.SupportedFunctions(-1) {
			return nil, fmt.Errorf("invalid scalr function provided %s", function)

		}
		expr, err := parseExpression(m["expr"].(map[string]any))
		if err != nil {
			return nil, err
		}
		sf := Expr.NewScalarFunction(Expr.FnToScalarFunction(function), expr)
		return sf, nil
	case "Alias":
		neededFields := []string{"name", "expr"}
		fieldTypes := []string{"string", "object"}
		err := containsFields(neededFields, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		err = correctFieldTypes(neededFields, fieldTypes, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		expr, err := parseExpression(m["expr"].(map[string]any))
		if err != nil {
			return nil, err
		}
		name := m["name"].(string)
		alias := Expr.NewAlias(expr, name)
		return alias, nil
	case "CastExpr":
		neededFields := []string{"expr", "to_type"}
		fieldTypes := []string{"object", "string"}
		err := containsFields(neededFields, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		err = correctFieldTypes(neededFields, fieldTypes, m)
		if err != nil {
			return nil, fmt.Errorf("malformed expression body: %v", err)
		}
		expr, err := parseExpression(m["expr"].(map[string]any))
		if err != nil {
			return nil, err
		}
		var T arrow.DataType
		switch m["to_type"].(string) {
		case "int":
			T = arrow.PrimitiveTypes.Int64
		case "string":
			T = arrow.BinaryTypes.String
		case "boolean":
			T = arrow.FixedWidthTypes.Boolean
		case "float64":
			T = arrow.PrimitiveTypes.Float64
		default:
			return nil, fmt.Errorf("invalid type provided.%v", m["to_type"])
		}
		cast := Expr.NewCastExpr(expr, T)
		return cast, nil
	default:
		return nil, fmt.Errorf("invalid expression: %v", m["expr_type"])
	}

}
func resolveInput(m jsonOBJ, plan *planMetaData) (operators.Operator, error) {
	const OperatorStr = "Operator"
	fields := []string{OperatorStr}
	if err := containsFields(fields, m); err != nil {
		return nil, err
	}
	opName := m[OperatorStr].(string)
	_, ok := m[opName]
	if !ok {
		return nil, fmt.Errorf("malformed json body.operator body does not contain %s's body", opName)
	}

	if err := correctFieldTypes([]string{OperatorStr, opName}, []string{"string", "object"}, m); err != nil {
		return nil, err
	}
	newOBJ := m[opName].(map[string]any)
	switch strings.ToLower(opName) {
	// base case, we hit a leaf node (source node)
	case "source": // return concrete base case here
		return parseSource(newOBJ, plan)
	case "project":
		return parseProject(newOBJ, plan)
	case "filter":
		return parseFilter(newOBJ, plan)
	case "distinct":
		return parseDistinct(newOBJ, plan)
	case "limit":
		return parseLimit(newOBJ, plan)
	case "sort":
		return parseSort(newOBJ, plan)
	case "aggregate":
	case "having":
	case "join":
	case "groupby":
	}

	return nil, nil
}

// check that all the fileds exist, if any are missing return and error indicating which fields are missing
// ignore any extra fields that may be present for now
func containsFields(fields []string, obj map[string]any) error {
	var missing []string

	for _, f := range fields {
		if _, ok := obj[f]; !ok {
			missing = append(missing, f)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf(
			"missing required fields: %s",
			strings.Join(missing, ", "),
		)
	}

	return nil
}

type misMatchTypes struct {
	idx              uint8
	fieldName        string
	value            any // from "%v" formating
	recievedType     string
	expectedDataType string
}

func correctFieldTypes(fields []string, fieldTypes []string, obj jsonOBJ) error {
	if len(fields) != len(fieldTypes) {
		return fmt.Errorf("fields and fieldTypes must have the same number of elements")
	}
	var misMatches []misMatchTypes

	// dont need to do _,ok  pattern here because we can assume contains fields is called before this one
	for i, field := range fields {
		value := obj[field]
		expected := fieldTypes[i]
		if !matchesExpectedType(value, expected) {
			misMatches = append(misMatches, misMatchTypes{
				idx:              uint8(i),
				fieldName:        field,
				value:            value,
				recievedType:     fmt.Sprintf("%T", value),
				expectedDataType: expected,
			})
		}

	}
	if len(misMatches) > 0 {
		return fmt.Errorf("all fields did not match their expected data types \t%#v", misMatches)

	}
	return nil // mismatch in field and their expected types, field1 is not of expected type T1
}

func matchesExpectedType(value any, expected string) bool {
	switch expected {
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "int":
		switch value.(type) {
		case float64, float32, int:
			return true
		default:
			return false
		}
	case "float64":
		fmt.Printf("hit float case second\n")
		_, ok := value.(float64)
		return ok
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		// Use reflection to check if it's any kind of slice/array
		return reflect.TypeOf(value).Kind() == reflect.Slice
	default:
		return false
	}
}

func validBinaryOp(s string) (Expr.BinaryOperator, error) {
	switch s {
	// arithmetic
	case "Addition":
		return Expr.BinaryOperator(Expr.Addition), nil
	case "Subtraction":
		return Expr.BinaryOperator(Expr.Subtraction), nil
	case "Multiplication":
		return Expr.BinaryOperator(Expr.Multiplication), nil
	case "Division":
		return Expr.BinaryOperator(Expr.Division), nil

	// comparison
	case "Equal":
		return Expr.BinaryOperator(Expr.Equal), nil
	case "NotEqual":
		return Expr.BinaryOperator(Expr.NotEqual), nil
	case "LessThan":
		return Expr.BinaryOperator(Expr.LessThan), nil
	case "LessThanOrEqual":
		return Expr.BinaryOperator(Expr.LessThanOrEqual), nil
	case "GreaterThan":
		return Expr.BinaryOperator(Expr.GreaterThan), nil
	case "GreaterThanOrEqual":
		return Expr.BinaryOperator(Expr.GreaterThanOrEqual), nil

	// logical
	case "And":
		return Expr.BinaryOperator(Expr.And), nil
	case "Or":
		return Expr.BinaryOperator(Expr.Or), nil

	// regex
	case "Like":
		return Expr.BinaryOperator(Expr.Like), nil

	default:
		return Expr.BinaryOperator(-1), fmt.Errorf("invalid binary operator: %s", s)
	}
}

// call strings.toLower before invoking this method
func validFN(s string) bool {
	switch s {
	case "sum", "count", "avg", "min", "max":
		return true
	default:
		return false
	}
}

func toAggrFn(s string) aggr.AggrFunc {
	switch s {
	case "sum":
		return aggr.Sum
	case "count":
		return aggr.Count
	case "avg":
		return aggr.Avg
	case "min":
		return aggr.Min
	case "max":
		return aggr.Max
	}
	fmt.Printf("got %s which is not supported", s)
	return -1
}

func generateAggrs(aggrs []map[string]any) ([]aggr.AggregateFunctions, error) {
	var globalAggrs []aggr.AggregateFunctions
	for _, a := range aggrs {
		fields := []string{"function", "expr"}
		err := containsFields(fields, a)
		if err != nil {
			return nil, err
		}
		err = correctFieldTypes(fields, []string{"string", "object"}, a)
		if err != nil {
			return nil, err
		}
		fn := strings.ToLower(a["function"].(string))
		if !validFN(fn) {
			return nil, fmt.Errorf("%s is not a valid aggregation method", fn)
		}
		expr, err := parseExpression(a["expr"].(map[string]any))
		if err != nil {
			return nil, err
		}
		globalAggrs = append(globalAggrs, aggr.NewAggregateFunctions(toAggrFn(fn), expr))
	}
	return globalAggrs, nil
}
