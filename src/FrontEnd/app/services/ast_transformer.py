from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class ASTTransformError(Exception):
    """Raised when AST transformation fails."""
    pass


class ASTTransformer:
    """Transforms C++ parser AST to backend IR format."""

    BINARY_OP_MAP = {
        "=": "Equal",
        "==": "Equal",
        "!=": "NotEqual",
        "<>": "NotEqual",
        "<": "LessThan",
        "<=": "LessThanOrEqual",
        ">": "GreaterThan",
        ">=": "GreaterThanOrEqual",
        "+": "Addition",
        "-": "Subtraction",
        "*": "Multiplication",
        "/": "Division",
        "%": "Modulo",
        "AND": "And",
        "OR": "Or",
        "||": "Concat",
    }

    LITERAL_TYPE_MAP = {
        "string": "string",
        "integer": "int",
        "float": "float",
        "boolean": "bool",
        "null": "null",
    }

    def __init__(self, s3_source: Optional[str] = None):
        self.s3_source = s3_source
        self._source_file_name: str = ""
        self._source_local: bool = False

    def _add_source_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Add file-name and local fields that the backend requires in every operator."""
        data["file-name"] = self._source_file_name
        data["local"] = self._source_local
        return data

    def transform(self, parser_ast: Dict[str, Any]) -> Dict[str, Any]:
        if parser_ast.get("error"):
            raise ASTTransformError(f"Cannot transform error AST: {parser_ast.get('message')}")

        if parser_ast.get("type") != "SelectStatement":
            raise ASTTransformError(f"Unsupported statement type: {parser_ast.get('type')}")

        operator_tree = self._transform_select_statement(parser_ast)
        return {"Emit": operator_tree}

    def _transform_select_statement(self, stmt: Dict[str, Any]) -> Dict[str, Any]:
        current_op = self._transform_from_clause(stmt.get("from", {}))

        where_clause = stmt.get("where")
        if where_clause:
            current_op = self._make_filter(current_op, where_clause)

        group_by = stmt.get("groupBy")
        select_clause = stmt.get("select", {})
        if group_by:
            current_op = self._transform_group_by(current_op, group_by, select_clause)
        elif self._has_aggregates(select_clause):
            current_op = self._transform_aggregate(current_op, select_clause)

        having_clause = stmt.get("having")
        if having_clause:
            current_op = self._make_having(current_op, having_clause)

        if not group_by and not self._has_aggregates(select_clause):
            current_op = self._transform_select_clause(current_op, select_clause)

        if select_clause.get("distinct"):
            current_op = self._make_distinct(current_op, select_clause)

        order_by = stmt.get("orderBy")
        if order_by:
            current_op = self._transform_order_by(current_op, order_by)

        limit_clause = stmt.get("limit")
        if limit_clause:
            current_op = self._transform_limit(current_op, limit_clause)

        return current_op

    def _transform_from_clause(self, from_clause: Dict[str, Any]) -> Dict[str, Any]:
        primary = from_clause.get("primary", {})
        joins = from_clause.get("joins", [])

        current_op = self._make_source(primary)

        for join in joins:
            right_source = self._make_source(join.get("table", {}))
            current_op = self._make_join(current_op, right_source, join)

        return current_op

    def _make_source(self, table_ref: Dict[str, Any]) -> Dict[str, Any]:
        table_type = table_ref.get("type")

        if table_type == "SubqueryTable":
            subquery = table_ref.get("query", {})
            return self._transform_select_statement(subquery)

        table_name = table_ref.get("name", "")
        schema = table_ref.get("schema", "")

        if self.s3_source:
            file_name = self.s3_source
            # Extract just the key from S3 URI (e.g., "s3://bucket/key" -> "key")
            if file_name.startswith("s3://"):
                parts = file_name.split("/", 3)  # ["s3:", "", "bucket", "key"]
                if len(parts) >= 4:
                    file_name = parts[3]
        elif schema:
            file_name = f"{schema}.{table_name}"
        else:
            file_name = table_name

        # Store source info for use by other operators
        self._source_file_name = file_name
        self._source_local = False

        return {
            "Operator": "Source",
            "Source": self._add_source_info({
                "source-node": {
                    "file-name": file_name,
                    "local": False
                }
            })
        }

    def _make_join(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any],
        join_clause: Dict[str, Any]
    ) -> Dict[str, Any]:
        join_type = join_clause.get("type", "INNER")
        condition = join_clause.get("condition")

        join_type_map = {
            "INNER": "Inner",
            "LEFT": "Left",
            "RIGHT": "Right",
            "FULL": "Full",
            "CROSS": "Cross",
        }
        mapped_type = join_type_map.get(join_type.upper(), "Inner")

        on_conditions = self._extract_join_conditions(condition) if condition else []

        return {
            "Operator": "Join",
            "Join": self._add_source_info({
                "left": left,
                "right": right,
                "join_type": mapped_type,
                "on": on_conditions
            })
        }

    def _extract_join_conditions(self, condition: Dict[str, Any]) -> List[Dict[str, Any]]:
        conditions = []

        if condition.get("type") == "BinaryExpr":
            op = condition.get("operator", "")
            left = condition.get("left", {})
            right = condition.get("right", {})

            if op in ("=", "=="):
                conditions.append({
                    "left": self._transform_expression(left),
                    "right": self._transform_expression(right)
                })
            elif op.upper() == "AND":
                conditions.extend(self._extract_join_conditions(left))
                conditions.extend(self._extract_join_conditions(right))

        return conditions

    def _make_filter(self, input_op: Dict[str, Any], condition: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "Operator": "Filter",
            "Filter": self._add_source_info({
                "input": input_op,
                "expression": self._transform_expression(condition)
            })
        }

    def _make_having(self, input_op: Dict[str, Any], condition: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "Operator": "Having",
            "Having": self._add_source_info({
                "input": input_op,
                "expression": self._transform_expression(condition)
            })
        }

    def _transform_select_clause(
        self,
        input_op: Dict[str, Any],
        select_clause: Dict[str, Any]
    ) -> Dict[str, Any]:
        items = select_clause.get("items", [])
        expressions = []

        for item in items:
            expr = item.get("expression", {})
            if expr.get("type") == "Star":
                expressions.append({"expr_type": "Star"})
            else:
                expressions.append(self._transform_expression(expr))

        return {
            "Operator": "Project",
            "Project": self._add_source_info({
                "input": input_op,
                "expressions": expressions
            })
        }

    def _make_distinct(
        self,
        input_op: Dict[str, Any],
        select_clause: Dict[str, Any]
    ) -> Dict[str, Any]:
        items = select_clause.get("items", [])
        expressions = []

        for item in items:
            expr = item.get("expression", {})
            if expr.get("type") != "Star":
                expressions.append(self._transform_expression(expr))

        return {
            "Operator": "Distinct",
            "Distinct": self._add_source_info({
                "input": input_op,
                "expressions": expressions
            })
        }

    def _has_aggregates(self, select_clause: Dict[str, Any]) -> bool:
        items = select_clause.get("items", [])
        for item in items:
            expr = item.get("expression", {})
            if self._is_aggregate(expr):
                return True
        return False

    def _is_aggregate(self, expr: Dict[str, Any]) -> bool:
        if expr.get("type") == "FunctionCall":
            func_name = expr.get("name", "").upper()
            return func_name in ("SUM", "COUNT", "AVG", "MIN", "MAX", "FIRST", "LAST")
        return False

    def _transform_aggregate(
        self,
        input_op: Dict[str, Any],
        select_clause: Dict[str, Any]
    ) -> Dict[str, Any]:
        items = select_clause.get("items", [])
        aggrs = []

        for item in items:
            expr = item.get("expression", {})
            alias = item.get("alias")

            if self._is_aggregate(expr):
                aggr = self._transform_aggregate_function(expr)
                if alias:
                    aggr["alias"] = alias
                aggrs.append(aggr)

        return {
            "Operator": "Aggregate",
            "Aggregate": self._add_source_info({
                "input": input_op,
                "aggrs": aggrs
            })
        }

    def _transform_group_by(
        self,
        input_op: Dict[str, Any],
        group_by: List[Dict[str, Any]],
        select_clause: Dict[str, Any]
    ) -> Dict[str, Any]:
        group_exprs = [self._transform_expression(expr) for expr in group_by]

        items = select_clause.get("items", [])
        aggrs = []

        for item in items:
            expr = item.get("expression", {})
            alias = item.get("alias")

            if self._is_aggregate(expr):
                aggr = self._transform_aggregate_function(expr)
                if alias:
                    aggr["alias"] = alias
                aggrs.append(aggr)

        return {
            "Operator": "GroupBy",
            "GroupBy": self._add_source_info({
                "input": input_op,
                "group_by": group_exprs,
                "aggrs": aggrs
            })
        }

    def _transform_aggregate_function(self, expr: Dict[str, Any]) -> Dict[str, Any]:
        # Backend expects lowercase aggregation function names: sum, count, avg, min, max
        func_name = expr.get("name", "").lower()
        args = expr.get("arguments", [])

        if args:
            column_expr = self._transform_expression(args[0])
        else:
            column_expr = {"expr_type": "Star"}

        return {
            "function": func_name,
            "expr": column_expr
        }

    def _transform_order_by(
        self,
        input_op: Dict[str, Any],
        order_by: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        by_items = []

        for item in order_by:
            expr = item.get("expression", {})
            ascending = item.get("ascending", True)

            by_items.append({
                "expr": self._transform_expression(expr),
                "asc": ascending
            })

        return {
            "Operator": "Sort",
            "Sort": self._add_source_info({
                "input": input_op,
                "by": by_items
            })
        }

    def _transform_limit(
        self,
        input_op: Dict[str, Any],
        limit_clause: Dict[str, Any]
    ) -> Dict[str, Any]:
        count_expr = limit_clause.get("count", {})

        if count_expr.get("type") == "Literal":
            limit_value = count_expr.get("value", 0)
        else:
            limit_value = 65535

        limit_value = min(int(limit_value), 65535)

        return {
            "Operator": "Limit",
            "Limit": self._add_source_info({
                "input": input_op,
                "limit": limit_value
            })
        }

    def _transform_expression(self, expr: Dict[str, Any]) -> Dict[str, Any]:
        expr_type = expr.get("type", "")

        if expr_type == "Identifier":
            return self._transform_identifier(expr)

        elif expr_type == "Literal":
            return self._transform_literal(expr)

        elif expr_type == "BinaryExpr":
            return self._transform_binary_expr(expr)

        elif expr_type == "UnaryExpr":
            return self._transform_unary_expr(expr)

        elif expr_type == "FunctionCall":
            return self._transform_function_call(expr)

        elif expr_type == "Star":
            table = expr.get("table")
            if table:
                return {"expr_type": "ColumnResolve", "name": f"{table}.*"}
            return {"expr_type": "Star"}

        elif expr_type == "IsNullExpr":
            return self._transform_is_null(expr)

        elif expr_type == "BetweenExpr":
            return self._transform_between(expr)

        elif expr_type == "InExpr":
            return self._transform_in_expr(expr)

        elif expr_type == "LikeExpr":
            return self._transform_like(expr)

        elif expr_type == "CaseExpr":
            return self._transform_case(expr)

        elif expr_type == "Subquery":
            return {"expr_type": "Subquery", "query": self._transform_select_statement(expr.get("query", {}))}

        else:
            logger.warning(f"Unknown expression type: {expr_type}")
            return {"expr_type": "Unknown", "original": expr}

    def _transform_identifier(self, expr: Dict[str, Any]) -> Dict[str, Any]:
        parts = expr.get("parts", [])
        name = ".".join(parts) if parts else ""

        return {
            "expr_type": "ColumnResolve",
            "name": name
        }

    def _transform_literal(self, expr: Dict[str, Any]) -> Dict[str, Any]:
        data_type = expr.get("dataType", "string")
        value = expr.get("value")

        lit_type = self.LITERAL_TYPE_MAP.get(data_type, "string")

        return {
            "expr_type": "LiteralResolve",
            "value": value,
            "lit_type": lit_type
        }

    def _transform_binary_expr(self, expr: Dict[str, Any]) -> Dict[str, Any]:
        op = expr.get("operator", "")
        left = expr.get("left", {})
        right = expr.get("right", {})

        mapped_op = self.BINARY_OP_MAP.get(op, self.BINARY_OP_MAP.get(op.upper(), op))

        return {
            "expr_type": "BinaryExpr",
            "op": mapped_op,
            "left": self._transform_expression(left),
            "right": self._transform_expression(right)
        }

    def _transform_unary_expr(self, expr: Dict[str, Any]) -> Dict[str, Any]:
        op = expr.get("operator", "")
        operand = expr.get("operand", {})

        if op.upper() == "NOT":
            return {
                "expr_type": "UnaryExpr",
                "op": "Not",
                "operand": self._transform_expression(operand)
            }
        elif op == "-":
            return {
                "expr_type": "UnaryExpr",
                "op": "Negate",
                "operand": self._transform_expression(operand)
            }
        else:
            return {
                "expr_type": "UnaryExpr",
                "op": op,
                "operand": self._transform_expression(operand)
            }

    def _transform_function_call(self, expr: Dict[str, Any]) -> Dict[str, Any]:
        func_name = expr.get("name", "")
        args = expr.get("arguments", [])
        distinct = expr.get("distinct", False)

        transformed_args = [self._transform_expression(arg) for arg in args]

        result = {
            "expr_type": "FunctionCall",
            "name": func_name,
            "arguments": transformed_args
        }

        if distinct:
            result["distinct"] = True

        return result

    def _transform_is_null(self, expr: Dict[str, Any]) -> Dict[str, Any]:
        value = expr.get("value", {})
        negated = expr.get("negated", False)

        result = {
            "expr_type": "IsNull",
            "value": self._transform_expression(value)
        }

        if negated:
            result["negated"] = True

        return result

    def _transform_between(self, expr: Dict[str, Any]) -> Dict[str, Any]:
        value = expr.get("value", {})
        low = expr.get("low", {})
        high = expr.get("high", {})
        negated = expr.get("negated", False)

        result = {
            "expr_type": "Between",
            "value": self._transform_expression(value),
            "low": self._transform_expression(low),
            "high": self._transform_expression(high)
        }

        if negated:
            result["negated"] = True

        return result

    def _transform_in_expr(self, expr: Dict[str, Any]) -> Dict[str, Any]:
        value = expr.get("value", {})
        in_list = expr.get("list", [])
        negated = expr.get("negated", False)

        result = {
            "expr_type": "In",
            "value": self._transform_expression(value),
            "list": [self._transform_expression(item) for item in in_list]
        }

        if negated:
            result["negated"] = True

        return result

    def _transform_like(self, expr: Dict[str, Any]) -> Dict[str, Any]:
        value = expr.get("value", {})
        pattern = expr.get("pattern", {})
        negated = expr.get("negated", False)

        result = {
            "expr_type": "Like",
            "value": self._transform_expression(value),
            "pattern": self._transform_expression(pattern)
        }

        if negated:
            result["negated"] = True

        return result

    def _transform_case(self, expr: Dict[str, Any]) -> Dict[str, Any]:
        operand = expr.get("operand")
        when_clauses = expr.get("whenClauses", [])
        else_expr = expr.get("else")

        result = {
            "expr_type": "Case",
            "when": []
        }

        if operand:
            result["operand"] = self._transform_expression(operand)

        for clause in when_clauses:
            result["when"].append({
                "condition": self._transform_expression(clause.get("condition", {})),
                "result": self._transform_expression(clause.get("result", {}))
            })

        if else_expr:
            result["else"] = self._transform_expression(else_expr)

        return result


def transform_ast(parser_ast: Dict[str, Any], s3_source: Optional[str] = None) -> Dict[str, Any]:
    transformer = ASTTransformer(s3_source=s3_source)
    return transformer.transform(parser_ast)
