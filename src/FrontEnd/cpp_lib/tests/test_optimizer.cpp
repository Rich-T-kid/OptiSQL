#include <catch2/catch_amalgamated.hpp>
#include "Optimizer.hpp"
#include "Parser.hpp"
#include "Lexer.hpp"

using namespace optisql;

namespace {

ExprPtr parseExpr(const std::string& sql) {
    std::string fullSql = "SELECT " + sql;
    Lexer lexer(fullSql);
    auto tokens = lexer.tokenize();
    REQUIRE(tokens.has_value());

    Parser parser(*tokens);
    auto result = parser.parse();
    REQUIRE(result.has_value());
    REQUIRE(!(*result)->select.items.empty());

    return std::move((*result)->select.items[0].expression);
}

SelectStatement parseStmt(const std::string& sql) {
    Lexer lexer(sql);
    auto tokens = lexer.tokenize();
    REQUIRE(tokens.has_value());

    Parser parser(*tokens);
    auto result = parser.parse();
    REQUIRE(result.has_value());

    return std::move(**result);
}

long getLongValue(const ExprPtr& expr) {
    auto* lit = std::get_if<LiteralExpr>(expr.get());
    REQUIRE(lit != nullptr);
    auto* val = std::get_if<long>(&lit->value);
    REQUIRE(val != nullptr);
    return *val;
}

double getDoubleValue(const ExprPtr& expr) {
    auto* lit = std::get_if<LiteralExpr>(expr.get());
    REQUIRE(lit != nullptr);
    if (auto* val = std::get_if<double>(&lit->value)) {
        return *val;
    }
    if (auto* val = std::get_if<long>(&lit->value)) {
        return static_cast<double>(*val);
    }
    FAIL("Expected numeric literal");
    return 0;
}

bool getBoolValue(const ExprPtr& expr) {
    auto* lit = std::get_if<LiteralExpr>(expr.get());
    REQUIRE(lit != nullptr);
    auto* val = std::get_if<bool>(&lit->value);
    REQUIRE(val != nullptr);
    return *val;
}

std::string getStringValue(const ExprPtr& expr) {
    auto* lit = std::get_if<LiteralExpr>(expr.get());
    REQUIRE(lit != nullptr);
    auto* val = std::get_if<std::string>(&lit->value);
    REQUIRE(val != nullptr);
    return *val;
}

bool isIdentifier(const ExprPtr& expr, const std::string& name) {
    auto* ident = std::get_if<IdentifierExpr>(expr.get());
    if (!ident) return false;
    if (ident->parts.empty()) return false;
    return ident->parts.back() == name;
}

bool isLiteral(const ExprPtr& expr) {
    return std::get_if<LiteralExpr>(expr.get()) != nullptr;
}

}

TEST_CASE("Constant folding - arithmetic", "[optimizer]") {
    Optimizer opt;

    SECTION("Integer addition") {
        auto expr = parseExpr("1 + 2");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 3);
    }

    SECTION("Integer subtraction") {
        auto expr = parseExpr("10 - 7");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 3);
    }

    SECTION("Integer multiplication") {
        auto expr = parseExpr("6 * 7");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 42);
    }

    SECTION("Integer division") {
        auto expr = parseExpr("20 / 4");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 5);
    }

    SECTION("Integer modulo") {
        auto expr = parseExpr("17 % 5");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 2);
    }

    SECTION("Chained operations") {
        auto expr = parseExpr("1 + 2 * 3");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 7);
    }

    SECTION("Nested parentheses") {
        auto expr = parseExpr("(1 + 2) * (3 + 4)");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 21);
    }

    SECTION("Division by zero not folded") {
        auto expr = parseExpr("10 / 0");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(std::get_if<BinaryExpr>(result.get()) != nullptr);
    }

    SECTION("Negative numbers") {
        auto expr = parseExpr("-5 + 10");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 5);
    }

    SECTION("Double negation") {
        auto expr = parseExpr("- -5");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 5);
    }
}

TEST_CASE("Constant folding - floating point", "[optimizer]") {
    Optimizer opt;

    SECTION("Float addition") {
        auto expr = parseExpr("1.5 + 2.5");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getDoubleValue(result) == Catch::Approx(4.0));
    }

    SECTION("Float division") {
        auto expr = parseExpr("10.0 / 4.0");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getDoubleValue(result) == Catch::Approx(2.5));
    }

    SECTION("Mixed int and float") {
        auto expr = parseExpr("5 + 2.5");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getDoubleValue(result) == Catch::Approx(7.5));
    }
}

TEST_CASE("Constant folding - comparisons", "[optimizer]") {
    Optimizer opt;

    SECTION("Equal - true") {
        auto expr = parseExpr("5 = 5");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getBoolValue(result) == true);
    }

    SECTION("Equal - false") {
        auto expr = parseExpr("5 = 6");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getBoolValue(result) == false);
    }

    SECTION("Not equal - true") {
        auto expr = parseExpr("5 != 6");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getBoolValue(result) == true);
    }

    SECTION("Less than - true") {
        auto expr = parseExpr("3 < 5");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getBoolValue(result) == true);
    }

    SECTION("Less than - false") {
        auto expr = parseExpr("5 < 3");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getBoolValue(result) == false);
    }

    SECTION("Greater than or equal") {
        auto expr = parseExpr("5 >= 5");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getBoolValue(result) == true);
    }
}

TEST_CASE("Constant folding - NOT", "[optimizer]") {
    Optimizer opt;

    SECTION("NOT TRUE") {
        auto expr = parseExpr("NOT TRUE");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getBoolValue(result) == false);
    }

    SECTION("NOT FALSE") {
        auto expr = parseExpr("NOT FALSE");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getBoolValue(result) == true);
    }
}

TEST_CASE("Constant folding - string concatenation", "[optimizer]") {
    Optimizer opt;

    SECTION("Simple concat") {
        auto expr = parseExpr("'hello' || ' world'");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getStringValue(result) == "hello world");
    }

    SECTION("Multiple concat") {
        auto expr = parseExpr("'a' || 'b' || 'c'");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getStringValue(result) == "abc");
    }
}

TEST_CASE("Numerical simplification", "[optimizer]") {
    Optimizer opt;

    SECTION("x + 0 = x") {
        auto expr = parseExpr("col + 0");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(isIdentifier(result, "col"));
    }

    SECTION("0 + x = x") {
        auto expr = parseExpr("0 + col");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(isIdentifier(result, "col"));
    }

    SECTION("x - 0 = x") {
        auto expr = parseExpr("col - 0");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(isIdentifier(result, "col"));
    }

    SECTION("x * 1 = x") {
        auto expr = parseExpr("col * 1");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(isIdentifier(result, "col"));
    }

    SECTION("1 * x = x") {
        auto expr = parseExpr("1 * col");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(isIdentifier(result, "col"));
    }

    SECTION("x * 0 = 0") {
        auto expr = parseExpr("col * 0");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 0);
    }

    SECTION("0 * x = 0") {
        auto expr = parseExpr("0 * col");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 0);
    }

    SECTION("x / 1 = x") {
        auto expr = parseExpr("col / 1");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(isIdentifier(result, "col"));
    }
}

TEST_CASE("Predicate simplification", "[optimizer]") {
    Optimizer opt;

    SECTION("NOT NOT x = x") {
        auto expr = parseExpr("NOT NOT col");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(isIdentifier(result, "col"));
    }

    SECTION("NULL IS NULL = TRUE") {
        auto expr = parseExpr("NULL IS NULL");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getBoolValue(result) == true);
    }

    SECTION("NULL IS NOT NULL = FALSE") {
        auto expr = parseExpr("NULL IS NOT NULL");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getBoolValue(result) == false);
    }

    SECTION("constant IS NULL = FALSE") {
        auto expr = parseExpr("5 IS NULL");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getBoolValue(result) == false);
    }

    SECTION("constant IS NOT NULL = TRUE") {
        auto expr = parseExpr("5 IS NOT NULL");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getBoolValue(result) == true);
    }
}

TEST_CASE("Combined optimizations", "[optimizer]") {
    Optimizer opt;

    SECTION("Fold then simplify: (1 + 1) * x") {
        auto expr = parseExpr("(1 + 1) * col");
        auto result = opt.optimizeExpression(std::move(expr));
        auto* binary = std::get_if<BinaryExpr>(result.get());
        REQUIRE(binary != nullptr);
        REQUIRE(getLongValue(binary->left) == 2);
        REQUIRE(isIdentifier(binary->right, "col"));
    }

    SECTION("Complex: (1 + 0) * (x + 0)") {
        auto expr = parseExpr("(1 + 0) * (col + 0)");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(isIdentifier(result, "col"));
    }
}

TEST_CASE("Statement optimization", "[optimizer]") {
    Optimizer opt;

    SECTION("Optimize WHERE clause") {
        auto stmt = parseStmt("SELECT a FROM t WHERE 1 + 1 = 2");
        auto result = opt.optimize(std::move(stmt));
        REQUIRE(result.where.has_value());
        REQUIRE(getBoolValue(result.where->condition) == true);
    }

    SECTION("Optimize SELECT expressions") {
        auto stmt = parseStmt("SELECT 1 + 2, 3 * 4 FROM t");
        auto result = opt.optimize(std::move(stmt));
        REQUIRE(result.select.items.size() == 2);
        REQUIRE(getLongValue(result.select.items[0].expression) == 3);
        REQUIRE(getLongValue(result.select.items[1].expression) == 12);
    }

    SECTION("Optimize HAVING clause") {
        auto stmt = parseStmt("SELECT a FROM t GROUP BY a HAVING 1 = 0");
        auto result = opt.optimize(std::move(stmt));
        REQUIRE(result.having.has_value());
        REQUIRE(getBoolValue(result.having->condition) == false);
    }

    SECTION("Optimize ORDER BY") {
        auto stmt = parseStmt("SELECT a FROM t ORDER BY 1 + 0");
        auto result = opt.optimize(std::move(stmt));
        REQUIRE(result.orderBy.has_value());
        REQUIRE(result.orderBy->items.size() == 1);
        REQUIRE(getLongValue(result.orderBy->items[0].expression) == 1);
    }

    SECTION("Optimize JOIN condition") {
        auto stmt = parseStmt("SELECT a FROM t1 JOIN t2 ON 1 = 1");
        auto result = opt.optimize(std::move(stmt));
        REQUIRE(result.from.has_value());
        REQUIRE(result.from->joins.size() == 1);
        REQUIRE(result.from->joins[0].condition.has_value());
        REQUIRE(getBoolValue(*result.from->joins[0].condition) == true);
    }
}

TEST_CASE("Edge cases", "[optimizer]") {
    Optimizer opt;

    SECTION("Empty expression") {
        ExprPtr expr = nullptr;
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(result == nullptr);
    }

    SECTION("Non-foldable expression unchanged") {
        auto expr = parseExpr("a + b");
        auto result = opt.optimizeExpression(std::move(expr));
        auto* binary = std::get_if<BinaryExpr>(result.get());
        REQUIRE(binary != nullptr);
        REQUIRE(isIdentifier(binary->left, "a"));
        REQUIRE(isIdentifier(binary->right, "b"));
    }

    SECTION("Function with constant args not folded") {
        auto expr = parseExpr("MAX(1, 2, 3)");
        auto result = opt.optimizeExpression(std::move(expr));
        auto* func = std::get_if<FunctionCallExpr>(result.get());
        REQUIRE(func != nullptr);
        REQUIRE(func->name == "MAX");
    }

    SECTION("CASE expression - conditions optimized") {
        auto expr = parseExpr("CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END");
        auto result = opt.optimizeExpression(std::move(expr));
        auto* caseExpr = std::get_if<CaseExpr>(result.get());
        REQUIRE(caseExpr != nullptr);
        REQUIRE(caseExpr->whenClauses.size() == 1);
        REQUIRE(getBoolValue(caseExpr->whenClauses[0].condition) == true);
    }

    SECTION("BETWEEN - bounds optimized") {
        auto expr = parseExpr("x BETWEEN 1 + 1 AND 2 + 2");
        auto result = opt.optimizeExpression(std::move(expr));
        auto* between = std::get_if<BetweenExpr>(result.get());
        REQUIRE(between != nullptr);
        REQUIRE(getLongValue(between->low) == 2);
        REQUIRE(getLongValue(between->high) == 4);
    }

    SECTION("IN - values optimized") {
        auto expr = parseExpr("x IN (1 + 1, 2 + 2)");
        auto result = opt.optimizeExpression(std::move(expr));
        auto* inExpr = std::get_if<InExpr>(result.get());
        REQUIRE(inExpr != nullptr);
        auto* values = std::get_if<std::vector<ExprPtr>>(&inExpr->list);
        REQUIRE(values != nullptr);
        REQUIRE(values->size() == 2);
        REQUIRE(getLongValue((*values)[0]) == 2);
        REQUIRE(getLongValue((*values)[1]) == 4);
    }
}

TEST_CASE("Ridiculous edge cases", "[optimizer]") {
    Optimizer opt;

    SECTION("Deeply nested arithmetic") {
        auto expr = parseExpr("((((1 + 1) + 1) + 1) + 1)");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 5);
    }

    SECTION("Many operations") {
        auto expr = parseExpr("1 + 2 - 3 + 4 - 5 + 6");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 5);
    }

    SECTION("Nested NOT") {
        auto expr = parseExpr("NOT NOT NOT NOT TRUE");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getBoolValue(result) == true);
    }

    SECTION("Quadruple negation") {
        auto expr = parseExpr("- - - -5");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 5);
    }

    SECTION("x * 0 + y * 0") {
        auto expr = parseExpr("a * 0 + b * 0");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 0);
    }

    SECTION("(x * 1) * 1 * 1") {
        auto expr = parseExpr("((col * 1) * 1) * 1");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(isIdentifier(result, "col"));
    }

    SECTION("Large constant computation") {
        auto expr = parseExpr("1000000 * 1000000");
        auto result = opt.optimizeExpression(std::move(expr));
        REQUIRE(getLongValue(result) == 1000000000000L);
    }
}
