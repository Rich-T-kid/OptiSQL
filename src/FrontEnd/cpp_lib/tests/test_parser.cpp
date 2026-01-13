#include <catch2/catch_amalgamated.hpp>
#include "Lexer.hpp"
#include "Parser.hpp"

auto parse(std::string_view sql) {
    Lexer lexer(sql);
    auto tokens = lexer.tokenize();
    REQUIRE(tokens.has_value());
    Parser parser(std::move(*tokens));
    return parser.parse();
}

TEST_CASE("Parser handles SELECT 1", "[parser]") {
    auto result = parse("SELECT 1");
    REQUIRE(result.has_value());
    CHECK(result.value()->select.items.size() == 1);
}

TEST_CASE("Parser handles SELECT *", "[parser]") {
    auto result = parse("SELECT *");
    REQUIRE(result.has_value());
    CHECK(result.value()->select.items.size() == 1);
    CHECK(std::holds_alternative<StarExpr>(*result.value()->select.items[0].expression));
}

TEST_CASE("Parser handles SELECT a", "[parser]") {
    auto result = parse("SELECT a");
    REQUIRE(result.has_value());
    auto& item = result.value()->select.items[0];
    REQUIRE(std::holds_alternative<IdentifierExpr>(*item.expression));
    CHECK(std::get<IdentifierExpr>(*item.expression).parts[0] == "a");
}

TEST_CASE("Parser handles SELECT a, b, c", "[parser]") {
    auto result = parse("SELECT a, b, c");
    REQUIRE(result.has_value());
    CHECK(result.value()->select.items.size() == 3);
}

TEST_CASE("Parser handles SELECT a AS x", "[parser]") {
    auto result = parse("SELECT a AS x");
    REQUIRE(result.has_value());
    CHECK(result.value()->select.items[0].alias == "x");
}

TEST_CASE("Parser handles SELECT a x (implicit alias)", "[parser]") {
    auto result = parse("SELECT a x");
    REQUIRE(result.has_value());
    CHECK(result.value()->select.items[0].alias == "x");
}

TEST_CASE("Parser handles SELECT DISTINCT a", "[parser]") {
    auto result = parse("SELECT DISTINCT a");
    REQUIRE(result.has_value());
    CHECK(result.value()->select.distinct == true);
}

TEST_CASE("Parser handles SELECT ALL a", "[parser]") {
    auto result = parse("SELECT ALL a");
    REQUIRE(result.has_value());
    CHECK(result.value()->select.distinct == false);
}

TEST_CASE("Parser handles SELECT * FROM t", "[parser]") {
    auto result = parse("SELECT * FROM t");
    REQUIRE(result.has_value());
    REQUIRE(result.value()->from.has_value());
    auto& source = result.value()->from->primary.source;
    REQUIRE(std::holds_alternative<TableRef::BaseTable>(source));
    CHECK(std::get<TableRef::BaseTable>(source).name == "t");
}

TEST_CASE("Parser handles SELECT a FROM schema.table", "[parser]") {
    auto result = parse("SELECT a FROM schema.table_name");
    REQUIRE(result.has_value());
    auto& source = result.value()->from->primary.source;
    REQUIRE(std::holds_alternative<TableRef::BaseTable>(source));
    auto& table = std::get<TableRef::BaseTable>(source);
    CHECK(table.schema == "schema");
    CHECK(table.name == "table_name");
}

TEST_CASE("Parser handles SELECT 1 + 2", "[parser]") {
    auto result = parse("SELECT 1 + 2");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->select.items[0].expression;
    REQUIRE(std::holds_alternative<BinaryExpr>(expr));
    CHECK(std::get<BinaryExpr>(expr).op == OperatorType::Plus);
}

TEST_CASE("Parser handles SELECT 1 + 2 * 3", "[parser]") {
    auto result = parse("SELECT 1 + 2 * 3");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->select.items[0].expression;
    REQUIRE(std::holds_alternative<BinaryExpr>(expr));
    auto& binary = std::get<BinaryExpr>(expr);
    CHECK(binary.op == OperatorType::Plus);
    REQUIRE(std::holds_alternative<BinaryExpr>(*binary.right));
    CHECK(std::get<BinaryExpr>(*binary.right).op == OperatorType::Multiply);
}

TEST_CASE("Parser handles SELECT (1 + 2) * 3", "[parser]") {
    auto result = parse("SELECT (1 + 2) * 3");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->select.items[0].expression;
    REQUIRE(std::holds_alternative<BinaryExpr>(expr));
    auto& binary = std::get<BinaryExpr>(expr);
    CHECK(binary.op == OperatorType::Multiply);
    REQUIRE(std::holds_alternative<BinaryExpr>(*binary.left));
    CHECK(std::get<BinaryExpr>(*binary.left).op == OperatorType::Plus);
}

TEST_CASE("Parser handles SELECT -1", "[parser]") {
    auto result = parse("SELECT -1");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->select.items[0].expression;
    REQUIRE(std::holds_alternative<UnaryExpr>(expr));
    CHECK(std::get<UnaryExpr>(expr).op == UnaryExpr::Op::Minus);
}

TEST_CASE("Parser handles SELECT NOT TRUE", "[parser]") {
    auto result = parse("SELECT NOT TRUE");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->select.items[0].expression;
    REQUIRE(std::holds_alternative<UnaryExpr>(expr));
    CHECK(std::get<UnaryExpr>(expr).op == UnaryExpr::Op::Not);
}

TEST_CASE("Parser handles WHERE clause", "[parser]") {
    auto result = parse("SELECT * FROM t WHERE 1 = 1");
    REQUIRE(result.has_value());
    REQUIRE(result.value()->where.has_value());
    auto& expr = *result.value()->where->condition;
    REQUIRE(std::holds_alternative<BinaryExpr>(expr));
    CHECK(std::get<BinaryExpr>(expr).op == OperatorType::Equal);
}

TEST_CASE("Parser handles WHERE a > 10 AND b < 20", "[parser]") {
    auto result = parse("SELECT * FROM t WHERE a > 10 AND b < 20");
    REQUIRE(result.has_value());
    REQUIRE(result.value()->where.has_value());
    auto& expr = *result.value()->where->condition;
    REQUIRE(std::holds_alternative<BinaryExpr>(expr));
}

TEST_CASE("Parser handles WHERE a > 10 OR b < 20", "[parser]") {
    auto result = parse("SELECT * FROM t WHERE a > 10 OR b < 20");
    REQUIRE(result.has_value());
    REQUIRE(result.value()->where.has_value());
}

TEST_CASE("Parser handles WHERE a BETWEEN 1 AND 10", "[parser]") {
    auto result = parse("SELECT * FROM t WHERE a BETWEEN 1 AND 10");
    REQUIRE(result.has_value());
    REQUIRE(result.value()->where.has_value());
    auto& expr = *result.value()->where->condition;
    REQUIRE(std::holds_alternative<BetweenExpr>(expr));
    CHECK(std::get<BetweenExpr>(expr).negated == false);
}

TEST_CASE("Parser handles WHERE a NOT BETWEEN 1 AND 10", "[parser]") {
    auto result = parse("SELECT * FROM t WHERE a NOT BETWEEN 1 AND 10");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->where->condition;
    REQUIRE(std::holds_alternative<BetweenExpr>(expr));
    CHECK(std::get<BetweenExpr>(expr).negated == true);
}

TEST_CASE("Parser handles WHERE a IN (1, 2, 3)", "[parser]") {
    auto result = parse("SELECT * FROM t WHERE a IN (1, 2, 3)");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->where->condition;
    REQUIRE(std::holds_alternative<InExpr>(expr));
    auto& in = std::get<InExpr>(expr);
    CHECK(in.negated == false);
    REQUIRE(std::holds_alternative<std::vector<ExprPtr>>(in.list));
    CHECK(std::get<std::vector<ExprPtr>>(in.list).size() == 3);
}

TEST_CASE("Parser handles WHERE a NOT IN (1, 2, 3)", "[parser]") {
    auto result = parse("SELECT * FROM t WHERE a NOT IN (1, 2, 3)");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->where->condition;
    REQUIRE(std::holds_alternative<InExpr>(expr));
    CHECK(std::get<InExpr>(expr).negated == true);
}

TEST_CASE("Parser handles WHERE a LIKE '%test%'", "[parser]") {
    auto result = parse("SELECT * FROM t WHERE a LIKE '%test%'");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->where->condition;
    REQUIRE(std::holds_alternative<LikeExpr>(expr));
    CHECK(std::get<LikeExpr>(expr).negated == false);
}

TEST_CASE("Parser handles WHERE a NOT LIKE 'x%'", "[parser]") {
    auto result = parse("SELECT * FROM t WHERE a NOT LIKE 'x%'");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->where->condition;
    REQUIRE(std::holds_alternative<LikeExpr>(expr));
    CHECK(std::get<LikeExpr>(expr).negated == true);
}

TEST_CASE("Parser handles JOIN", "[parser]") {
    auto result = parse("SELECT * FROM a JOIN b ON a.id = b.id");
    REQUIRE(result.has_value());
    REQUIRE(result.value()->from.has_value());
    CHECK(result.value()->from->joins.size() == 1);
    CHECK(result.value()->from->joins[0].type == JoinType::Inner);
}

TEST_CASE("Parser handles INNER JOIN", "[parser]") {
    auto result = parse("SELECT * FROM a INNER JOIN b ON a.id = b.id");
    REQUIRE(result.has_value());
    CHECK(result.value()->from->joins[0].type == JoinType::Inner);
}

TEST_CASE("Parser handles LEFT JOIN", "[parser]") {
    auto result = parse("SELECT * FROM a LEFT JOIN b ON a.id = b.id");
    REQUIRE(result.has_value());
    CHECK(result.value()->from->joins[0].type == JoinType::Left);
}

TEST_CASE("Parser handles RIGHT JOIN", "[parser]") {
    auto result = parse("SELECT * FROM a RIGHT JOIN b ON a.id = b.id");
    REQUIRE(result.has_value());
    CHECK(result.value()->from->joins[0].type == JoinType::Right);
}

TEST_CASE("Parser handles multiple JOINs", "[parser]") {
    auto result = parse("SELECT * FROM a JOIN b ON a.x = b.x JOIN c ON b.y = c.y");
    REQUIRE(result.has_value());
    CHECK(result.value()->from->joins.size() == 2);
}

TEST_CASE("Parser handles GROUP BY", "[parser]") {
    auto result = parse("SELECT a, COUNT(*) FROM t GROUP BY a");
    REQUIRE(result.has_value());
    REQUIRE(result.value()->groupBy.has_value());
    CHECK(result.value()->groupBy->expressions.size() == 1);
}

TEST_CASE("Parser handles GROUP BY multiple columns", "[parser]") {
    auto result = parse("SELECT a, b, SUM(c) FROM t GROUP BY a, b");
    REQUIRE(result.has_value());
    CHECK(result.value()->groupBy->expressions.size() == 2);
}

TEST_CASE("Parser handles HAVING", "[parser]") {
    auto result = parse("SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 5");
    REQUIRE(result.has_value());
    REQUIRE(result.value()->having.has_value());
}

TEST_CASE("Parser handles ORDER BY", "[parser]") {
    auto result = parse("SELECT * FROM t ORDER BY a");
    REQUIRE(result.has_value());
    REQUIRE(result.value()->orderBy.has_value());
    CHECK(result.value()->orderBy->items.size() == 1);
    CHECK(result.value()->orderBy->items[0].ascending == true);
}

TEST_CASE("Parser handles ORDER BY DESC", "[parser]") {
    auto result = parse("SELECT * FROM t ORDER BY a DESC");
    REQUIRE(result.has_value());
    CHECK(result.value()->orderBy->items[0].ascending == false);
}

TEST_CASE("Parser handles ORDER BY multiple columns", "[parser]") {
    auto result = parse("SELECT * FROM t ORDER BY a ASC, b DESC");
    REQUIRE(result.has_value());
    CHECK(result.value()->orderBy->items.size() == 2);
    CHECK(result.value()->orderBy->items[0].ascending == true);
    CHECK(result.value()->orderBy->items[1].ascending == false);
}

TEST_CASE("Parser handles LIMIT", "[parser]") {
    auto result = parse("SELECT * FROM t LIMIT 10");
    REQUIRE(result.has_value());
    REQUIRE(result.value()->limit.has_value());
    auto& count = *result.value()->limit->count;
    REQUIRE(std::holds_alternative<LiteralExpr>(count));
    CHECK(std::get<long>(std::get<LiteralExpr>(count).value) == 10);
}

TEST_CASE("Parser handles LIMIT with OFFSET", "[parser]") {
    auto result = parse("SELECT * FROM t LIMIT 10 OFFSET 20");
    REQUIRE(result.has_value());
    REQUIRE(result.value()->limit.has_value());
    REQUIRE(result.value()->limit->offset.has_value());
}

TEST_CASE("Parser handles COUNT(*)", "[parser]") {
    auto result = parse("SELECT COUNT(*) FROM t");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->select.items[0].expression;
    REQUIRE(std::holds_alternative<FunctionCallExpr>(expr));
    auto& func = std::get<FunctionCallExpr>(expr);
    CHECK(func.name == "COUNT");
    CHECK(func.arguments.size() == 1);
}

TEST_CASE("Parser handles COUNT(DISTINCT a)", "[parser]") {
    auto result = parse("SELECT COUNT(DISTINCT a) FROM t");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->select.items[0].expression;
    REQUIRE(std::holds_alternative<FunctionCallExpr>(expr));
    CHECK(std::get<FunctionCallExpr>(expr).distinct == true);
}

TEST_CASE("Parser handles aggregate functions", "[parser]") {
    auto result = parse("SELECT SUM(a), AVG(b), MIN(c), MAX(d) FROM t");
    REQUIRE(result.has_value());
    CHECK(result.value()->select.items.size() == 4);
}

TEST_CASE("Parser handles CASE WHEN", "[parser]") {
    auto result = parse("SELECT CASE WHEN a > 0 THEN 'pos' ELSE 'neg' END FROM t");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->select.items[0].expression;
    REQUIRE(std::holds_alternative<CaseExpr>(expr));
    auto& caseExpr = std::get<CaseExpr>(expr);
    CHECK(caseExpr.whenClauses.size() == 1);
    CHECK(caseExpr.elseClause.has_value());
}

TEST_CASE("Parser handles CASE with multiple WHEN", "[parser]") {
    auto result = parse("SELECT CASE WHEN a > 0 THEN 'pos' WHEN a = 0 THEN 'zero' ELSE 'neg' END FROM t");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->select.items[0].expression;
    REQUIRE(std::holds_alternative<CaseExpr>(expr));
    CHECK(std::get<CaseExpr>(expr).whenClauses.size() == 2);
}

TEST_CASE("Parser handles simple CASE", "[parser]") {
    auto result = parse("SELECT CASE a WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->select.items[0].expression;
    REQUIRE(std::holds_alternative<CaseExpr>(expr));
    auto& caseExpr = std::get<CaseExpr>(expr);
    CHECK(caseExpr.operand.has_value());
}

TEST_CASE("Parser handles CAST", "[parser]") {
    auto result = parse("SELECT CAST(a AS INT) FROM t");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->select.items[0].expression;
    REQUIRE(std::holds_alternative<CastExpr>(expr));
    CHECK(std::get<CastExpr>(expr).targetType == KeywordType::INT);
}

TEST_CASE("Parser handles nested CAST", "[parser]") {
    auto result = parse("SELECT CAST(CAST(a AS TEXT) AS INT) FROM t");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->select.items[0].expression;
    REQUIRE(std::holds_alternative<CastExpr>(expr));
    auto& inner = *std::get<CastExpr>(expr).value;
    REQUIRE(std::holds_alternative<CastExpr>(inner));
}

TEST_CASE("Parser handles IN subquery", "[parser]") {
    auto result = parse("SELECT * FROM t WHERE a IN (SELECT b FROM s)");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->where->condition;
    REQUIRE(std::holds_alternative<InExpr>(expr));
    auto& in = std::get<InExpr>(expr);
    REQUIRE(std::holds_alternative<StmtPtr>(in.list));
}

TEST_CASE("Parser handles EXISTS", "[parser]") {
    auto result = parse("SELECT * FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.id)");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->where->condition;
    REQUIRE(std::holds_alternative<ExistsExpr>(expr));
}

TEST_CASE("Parser handles subquery in FROM", "[parser]") {
    auto result = parse("SELECT * FROM (SELECT a, b FROM t) AS sub");
    REQUIRE(result.has_value());
    auto& source = result.value()->from->primary.source;
    REQUIRE(std::holds_alternative<TableRef::SubqueryTable>(source));
    CHECK(std::get<TableRef::SubqueryTable>(source).alias == "sub");
}

TEST_CASE("Parser handles UNION", "[parser]") {
    auto result = parse("SELECT a FROM t1 UNION SELECT b FROM t2");
    REQUIRE(result.has_value());
    CHECK(result.value()->setOp == SetOperation::Union);
    CHECK(result.value()->rightQuery != nullptr);
}

TEST_CASE("Parser handles UNION ALL", "[parser]") {
    auto result = parse("SELECT a FROM t1 UNION ALL SELECT b FROM t2");
    REQUIRE(result.has_value());
    CHECK(result.value()->setOp == SetOperation::UnionAll);
}

TEST_CASE("Parser handles INTERSECT", "[parser]") {
    auto result = parse("SELECT a FROM t1 INTERSECT SELECT b FROM t2");
    REQUIRE(result.has_value());
    CHECK(result.value()->setOp == SetOperation::Intersect);
}

TEST_CASE("Parser handles EXCEPT", "[parser]") {
    auto result = parse("SELECT a FROM t1 EXCEPT SELECT b FROM t2");
    REQUIRE(result.has_value());
    CHECK(result.value()->setOp == SetOperation::Except);
}

TEST_CASE("Parser handles IS NULL", "[parser]") {
    auto result = parse("SELECT * FROM t WHERE a IS NULL");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->where->condition;
    REQUIRE(std::holds_alternative<IsNullExpr>(expr));
    CHECK(std::get<IsNullExpr>(expr).negated == false);
}

TEST_CASE("Parser handles IS NOT NULL", "[parser]") {
    auto result = parse("SELECT * FROM t WHERE a IS NOT NULL");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->where->condition;
    REQUIRE(std::holds_alternative<IsNullExpr>(expr));
    CHECK(std::get<IsNullExpr>(expr).negated == true);
}

TEST_CASE("Parser handles NULL literal", "[parser]") {
    auto result = parse("SELECT NULL");
    REQUIRE(result.has_value());
    auto& expr = *result.value()->select.items[0].expression;
    REQUIRE(std::holds_alternative<LiteralExpr>(expr));
    CHECK(std::holds_alternative<Null>(std::get<LiteralExpr>(expr).value));
}

TEST_CASE("Parser handles deeply nested parens", "[parser]") {
    auto result = parse("SELECT ((((((((((1))))))))))");
    REQUIRE(result.has_value());
}

TEST_CASE("Parser handles many columns", "[parser]") {
    auto result = parse("SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15");
    REQUIRE(result.has_value());
    CHECK(result.value()->select.items.size() == 15);
}

TEST_CASE("Parser handles complex nested query", "[parser]") {
    auto result = parse("SELECT * FROM t WHERE a IN (SELECT b FROM s WHERE c IN (SELECT d FROM r))");
    REQUIRE(result.has_value());
}

TEST_CASE("Parser handles string concatenation", "[parser]") {
    auto result = parse("SELECT a || b || c");
    REQUIRE(result.has_value());
}

TEST_CASE("Parser error: missing columns", "[parser][error]") {
    auto result = parse("SELECT FROM t");
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("Parser error: missing table", "[parser][error]") {
    auto result = parse("SELECT * FROM");
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("Parser error: incomplete WHERE", "[parser][error]") {
    auto result = parse("SELECT * FROM t WHERE");
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("Parser error: incomplete CASE", "[parser][error]") {
    auto result = parse("SELECT CASE WHEN a > 0 THEN");
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("Parser error: incomplete CAST", "[parser][error]") {
    auto result = parse("SELECT CAST(a AS) FROM t");
    REQUIRE_FALSE(result.has_value());
}
