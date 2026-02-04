package substrait

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"opti-sql-go/Expr"
	"opti-sql-go/config"
	"opti-sql-go/operators"
	"opti-sql-go/operators/aggr"
	"opti-sql-go/operators/filter"
	"opti-sql-go/operators/join"
	"opti-sql-go/operators/project"
	"os"
	"reflect"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"go.uber.org/zap"
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

type Emiter struct {
	emitOperator operators.Operator
	p            *planMetaData
}

func (e *Emiter) consumeAll() (*operators.RecordBatch, error) {
	logger := config.GetLogger()
	var results *operators.RecordBatch
	logger.Info("Starting consumeAll", zap.String("operator", e.emitOperator.Name()), zap.String("plan_id", e.p.id))
	logger.Debug("Inner operator name", zap.String("operator", e.emitOperator.Name()))
	mem := memory.NewGoAllocator()
	iterationCount := 0
	for {
		intermediate, err := e.emitOperator.Next(math.MaxInt16)
		if err != nil {
			if errors.Is(err, io.EOF) {
				logger.Info("Reached EOF", zap.Int("iterations", iterationCount))
				break
			}
			logger.Error("Error fetching next batch", zap.Error(err), zap.Int("iteration", iterationCount))
			return nil, err
		}
		iterationCount++
		// first iteration set results to the intermediate results
		if results == nil {
			logger.Info("First batch received", zap.Uint64("rows", intermediate.RowCount), zap.Int("columns", len(intermediate.Columns)))
			results = intermediate
			continue
		}
		// otherwise just append for each idx
		logger.Debug("Concatenating batch", zap.Uint64("new_rows", intermediate.RowCount), zap.Uint64("total_rows", results.RowCount))
		for i := range intermediate.Columns {
			oldArr := results.Columns[i]
			newArr := intermediate.Columns[i]
			joinArr, err := array.Concatenate([]arrow.Array{oldArr, newArr}, mem)
			if err != nil {
				logger.Error("Failed to concatenate arrays", zap.Error(err), zap.Int("column_index", i))
				return nil, err
			}
			results.Columns[i] = joinArr
		}
		results.RowCount += intermediate.RowCount

	}
	// delete source files
	logger.Info("Cleaning up local files", zap.Int("file_count", len(e.p.localFileNames)))
	for _, file := range e.p.localFileNames {
		if err := os.Remove(file); err != nil {
			logger.Error("Failed to delete local file", zap.Error(err), zap.String("file", file))
			return nil, err
		}
		logger.Debug("Deleted local file", zap.String("file", file))
	}
	_ = e.emitOperator.Close()
	logger.Info("consumeAll completed", zap.Uint64("total_rows", results.RowCount), zap.Int("total_columns", len(results.Columns)))
	return results, nil
}

// post-order: children first, then your name.
// NOTE: This assumes every operator you care about has exactly ONE input child in a field named `Input`
// (Join is the common exception: Left/Right).

type planMetaData struct {
	id             string
	localFileNames []string // check if empty before deleting the file
}

func newPlanMetaData(id string) *planMetaData {
	return &planMetaData{id: id}

}

// first turn into json. The plan should fit into ram to consume it all
func consumePlan(r io.Reader, p *planMetaData) (*Emiter, error) {
	logger := config.GetLogger()
	logger.Info("Starting plan consumption", zap.String("plan_id", p.id))

	contents, err := io.ReadAll(r)
	if err != nil {
		logger.Error("Failed to read plan", zap.Error(err))
		return nil, err
	}
	logger.Debug("Plan read successfully", zap.Int("bytes", len(contents)))

	inMemoryRepr := make(jsonOBJ)
	err = json.Unmarshal(contents, &inMemoryRepr)
	if err != nil {
		logger.Error("Failed to unmarshal JSON plan", zap.Error(err))
		return nil, ErrInvalidSubstraitPlan(err)
	}
	if len(inMemoryRepr) != 1 {
		logger.Error("Malformed plan body", zap.Int("root_count", len(inMemoryRepr)))
		return nil, ErrMalformedEmitBody
	}
	_, exist := inMemoryRepr["Emit"] // TODO! standerdize the spelling and casing of this or else everythign else will break
	if !exist {
		logger.Error("Missing Emit operator")
		return nil, ErrMissingEmitOperator
	}
	tree, ok := inMemoryRepr["Emit"].(map[string]any)
	if !ok {
		logger.Error("Invalid Emit children type")
		return nil, ErrInvalidEmitChildren
	}
	logger.Info("Plan structure validated, building operator tree")
	return buildTree(tree, p)
}

func buildTree(m jsonOBJ, plan *planMetaData) (*Emiter, error) {
	logger := config.GetLogger()
	//key=Operator , value=arguments to that operator

	// the tree needs to be built from the bottom up. Recurse all the way down until you reach a leaf node || key == "Source"
	const operator = "Operator"

	if err := containsFields([]string{operator}, m); err != nil {
		logger.Error("Missing operator field in tree node", zap.Error(err))
		return nil, err
	}
	if err := correctFieldTypes([]string{operator}, []string{"string"}, m); err != nil {
		logger.Error("Invalid operator field type", zap.Error(err))
		return nil, err
	}

	operatorNode := m[operator].(string)
	logger.Info("Building operator", zap.String("operator_type", operatorNode))
	body := m[operatorNode].(map[string]any)
	var op operators.Operator
	switch strings.ToLower(operatorNode) {
	case "filter":
		filterOP, err := parseFilter(body, plan)
		if err != nil {
			logger.Error("Failed to build filter operator", zap.Error(err))
			return nil, ErrBuildTreeFailed("filter", err.Error())
		}
		op = filterOP
		logger.Info("Filter operator built successfully")
		return &Emiter{op, plan}, nil
	case "project":
		projectOP, err := parseProject(body, plan)
		if err != nil {
			logger.Error("Failed to build project operator", zap.Error(err))
			return nil, ErrBuildTreeFailed("project", err.Error())
		}
		op = projectOP
		logger.Info("Project operator built successfully")
		return &Emiter{op, plan}, nil
	case "sort":
		sortOP, err := parseSort(body, plan)
		if err != nil {
			logger.Error("Failed to build sort operator", zap.Error(err))
			return nil, ErrBuildTreeFailed("sort", err.Error())
		}
		op = sortOP
		logger.Info("Sort operator built successfully")
		return &Emiter{op, plan}, nil

	case "distinct":
		distinctOP, err := parseDistinct(body, plan)
		if err != nil {
			logger.Error("Failed to build distinct operator", zap.Error(err))
			return nil, ErrBuildTreeFailed("distinct", err.Error())
		}
		op = distinctOP
		logger.Info("Distinct operator built successfully")
		return &Emiter{op, plan}, nil
	case "limit":
		limitOP, err := parseLimit(body, plan)
		if err != nil {
			logger.Error("Failed to build limit operator", zap.Error(err))
			return nil, ErrBuildTreeFailed("limit", err.Error())
		}
		op = limitOP
		logger.Info("Limit operator built successfully")
		return &Emiter{op, plan}, nil
	case "aggregate":
		aggrOP, err := parseSingleAggr(body, plan)
		if err != nil {
			logger.Error("Failed to build aggregate operator", zap.Error(err))
			return nil, ErrBuildTreeFailed("single-aggr", err.Error())
		}
		op = aggrOP
		logger.Info("Aggregate operator built successfully")
		return &Emiter{op, plan}, nil
	case "groupby":
		groupByOP, err := parseGroupBy(body, plan)
		if err != nil {
			logger.Error("Failed to build groupby operator", zap.Error(err))
			return nil, ErrBuildTreeFailed("group-by", err.Error())
		}
		op = groupByOP
		logger.Info("GroupBy operator built successfully")
		return &Emiter{op, plan}, err

	case "join":
		joinOP, err := parseJoin(body, plan)
		if err != nil {
			logger.Error("Failed to build join operator", zap.Error(err))
			return nil, ErrBuildTreeFailed("join", err.Error())
		}
		op = joinOP
		logger.Info("Join operator built successfully")
		return &Emiter{op, plan}, err

	case "source", "expression": // invalid branch
		//(1) Source:cannot directy return from source
		//(2) expressions: cannot directy return expressions, need to call project on top
		logger.Error("Invalid operator cannot be called before Emit", zap.String("operator", operatorNode))
		return nil, ErrInvalidOperator(operatorNode)
	}
	logger.Error("Unknown operator type", zap.String("operator", operatorNode))
	return nil, ErrBuildTreeFailed("unknown", "no valid operator found in logical plan")
}
func parseSource(sourceOBJ jsonOBJ, plan *planMetaData) (operators.Operator, error) {
	logger := config.GetLogger()
	fields := []string{"file-name", "local"}
	err := containsFields(fields, sourceOBJ)
	if err != nil {
		logger.Error("Missing required fields in source", zap.Error(err))
		return nil, err
	}
	err = correctFieldTypes(fields, []string{"string", "boolean"}, sourceOBJ)
	if err != nil {
		logger.Error("Invalid field types in source", zap.Error(err))
		return nil, err
	}
	name := sourceOBJ["file-name"].(string)
	logger.Info("Parsing source", zap.String("file_name", name))
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
	plan.localFileNames = append(plan.localFileNames, fmt.Sprintf("%s/%s", curDir, localFile.Name()))
	switch kind {
	case "csv":
		csvRootNode, err := project.NewProjectCSVLeaf(localFile)
		if err != nil {
			logger.Error("Failed to create CSV source", zap.Error(err))
			return nil, err
		}
		logger.Info("CSV source created successfully", zap.String("file", name))
		return csvRootNode, nil
	case "parquet":
		parquetRootNode, err := project.NewParquetSource(localFile)
		if err != nil {
			logger.Error("Failed to create Parquet source", zap.Error(err))
			return nil, err
		}
		logger.Info("Parquet source created successfully", zap.String("file", name))
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
	inp := filterOBJ["input"].(map[string]any)
	input, err := resolveInput(inp, plan)
	if err != nil {
		return nil, err
	}
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
	switch exprs := projectOBJ["expressions"].(type) {
	case []any:
		for i, raw := range exprs {
			m, ok := raw.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expressions[%d] invalid type, expected object but got %T", i, raw)
			}
			e, err := parseExpression(m)
			if err != nil {
				return nil, err
			}
			expres = append(expres, e)
		}
		// (tests)
	case []map[string]any:
		for i, m := range exprs {
			e, err := parseExpression(m)
			if err != nil {
				return nil, err
			}
			expres = append(expres, e)
			_ = i
		}

	default:
		return nil, fmt.Errorf("expressions field has invalid type, expected array but got %T", projectOBJ["expressions"])
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
	var byField []map[string]any

	switch v := sortOBJ["by"].(type) {
	case []any:
		byField = make([]map[string]any, 0, len(v))
		for i, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("sort::by[%d] is malformed, expected object but got %T", i, item)
			}
			byField = append(byField, m)
		}

	case []map[string]any:
		// Go-literal tests may already have the correct type
		byField = v

	default:
		return nil, fmt.Errorf("sort::by field is malformed, should be an array of objects, got %T", sortOBJ["by"])
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
	switch exprs := distinctOBJ["expressions"].(type) {
	case []any:
		for i, raw := range exprs {
			m, ok := raw.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expressions[%d] invalid type, expected object but got %T", i, raw)
			}
			e, err := parseExpression(m)
			if err != nil {
				return nil, err
			}
			expres = append(expres, e)
		}

	case []map[string]any:
		for i, m := range exprs {
			e, err := parseExpression(m)
			if err != nil {
				return nil, err
			}
			expres = append(expres, e)
			_ = i
		}
	default:
		return nil, fmt.Errorf("expressions field has invalid type, expected array but got %T", distinctOBJ["expressions"])
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
		// try to parse as float
		l, ok1 := limitOBJ["limit"].(float64)
		if !ok1 {
			return nil, fmt.Errorf("limit field is not the correct type: true Type %T", limitOBJ["limit"])
		}
		//workeds so cast to int
		limit = int(l)
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
	var res []map[string]any

	switch agVal := aggrOBJ["aggrs"].(type) {
	case []any:
		res = make([]map[string]any, 0, len(agVal))
		for i, item := range agVal {
			v, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("aggrs[%d] malformed, expected object but got %T", i, item)
			}
			res = append(res, v)
		}
	case []map[string]any:
		res = agVal

	default:
		return nil, fmt.Errorf("aggrs malformed, should be an array of aggregations: got %T", aggrOBJ["aggrs"])
	}
	globalAggrs, err := generateAggrs(res)
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

	// ---- group_by: accept []any (json) OR []map[string]any (go literals)
	switch gbVal := groupbyOBJ["group_by"].(type) {
	case []any:
		for i, gb := range gbVal {
			m, ok := gb.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("group_by[%d] malformed, expected object but got %T", i, gb)
			}
			e, err := parseExpression(m)
			if err != nil {
				return nil, err
			}
			groupByStatments = append(groupByStatments, e)
		}

	case []map[string]any:
		for i, m := range gbVal {
			e, err := parseExpression(m)
			if err != nil {
				return nil, err
			}
			groupByStatments = append(groupByStatments, e)
			_ = i
		}

	default:
		return nil, fmt.Errorf("group by statements are malformed, should be an array of expressions: got %T", groupbyOBJ["group_by"])
	}
	// ---- aggrs: accept []any (json) OR []map[string]any (go literals)
	var res []map[string]any

	switch agVal := groupbyOBJ["aggrs"].(type) {
	case []any:
		res = make([]map[string]any, 0, len(agVal))
		for i, item := range agVal {
			v, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("aggrs[%d] malformed, expected object but got %T", i, item)
			}
			res = append(res, v)
		}
	case []map[string]any:
		res = agVal

	default:
		return nil, fmt.Errorf("aggrs malformed, should be an array of aggregations: got %T", groupbyOBJ["aggrs"])
	}
	aggrs, err := generateAggrs(res)
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
	rawOn, ok := joinOBJ["on"]
	if !ok {
		return nil, fmt.Errorf("join::on field is missing")
	}

	var onClauses []map[string]any

	switch v := rawOn.(type) {
	case []any:
		onClauses = make([]map[string]any, 0, len(v))
		for i, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("join::on[%d] malformed, expected object but got %T", i, item)
			}
			onClauses = append(onClauses, m)
		}
	case []map[string]any:
		onClauses = v
	default:
		return nil, fmt.Errorf("join::on field is malformed, expected array of objects, got %T", rawOn)
	}
	jc, err := clauseParer(onClauses)
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
	logger := config.GetLogger()
	// grab tje expr_type and then parse based on that
	logger.Debug("JSON object passed in for expression parsing", zap.Any("json_object", m))
	err := containsFields([]string{"expr_type"}, m)
	if err != nil {
		logger.Error("Malformed expression: missing expr_type", zap.Error(err))
		return nil, fmt.Errorf("malformed expression body. Doesnt contain expr_type field")
	}
	exprType := m["expr_type"].(string)
	logger.Debug("Parsing expression", zap.String("expr_type", exprType))
	switch exprType {
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
		logger.Debug("Parsing LiteralResolve", zap.Any("raw_value", m["value"]), zap.Any("lit_type", m["lit_type"]))
		switch m["lit_type"].(string) {
		case "int":
			arrowType = arrow.PrimitiveTypes.Int64
			switch val := m["value"].(type) {
			case int:
				value = int64(val)
			case float64:
				value = int64(val)
			}
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
		logger.Debug("BinaryExpr created", zap.String("expression", fmt.Sprintf("%v", binaryExpression)))
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
		return parseSingleAggr(newOBJ, plan)
	case "having":
		return parseHaving(newOBJ, plan)
	case "join":
		return parseJoin(newOBJ, plan)
	case "groupby":
		return parseGroupBy(newOBJ, plan)
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
	logger := config.GetLogger()
	logger.Error("Unsupported aggregation function", zap.String("function", s))
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
