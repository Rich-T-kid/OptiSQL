#include <catch2/catch_amalgamated.hpp>
#include "SQLParser.hpp"

using namespace optisql;

TEST_CASE("Full pipeline - simple queries", "[integration]") {
    SQLParser parser;

    SECTION("Simple SELECT") {
        auto result = parser.parse("SELECT a, b FROM users");
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast["type"] == "SelectStatement");
        REQUIRE(ast["select"]["items"].size() == 2);
    }

    SECTION("SELECT with WHERE") {
        auto result = parser.parse("SELECT name FROM users WHERE id = 1");
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast.contains("where"));
        REQUIRE(ast["where"]["type"] == "BinaryExpr");
    }

    SECTION("SELECT *") {
        auto result = parser.parse("SELECT * FROM products");
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast["select"]["items"].size() == 1);
        REQUIRE(ast["select"]["items"][0]["expression"]["type"] == "Star");
    }
}

TEST_CASE("Full pipeline - optimization", "[integration]") {
    SQLParser parser;

    SECTION("Constant folding in WHERE") {
        auto result = parser.parse("SELECT a FROM t WHERE 1 + 1 = 2");
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast["where"]["type"] == "Literal");
        REQUIRE(ast["where"]["value"] == true);
    }

    SECTION("Numerical simplification") {
        auto result = parser.parse("SELECT col * 1 FROM t");
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        auto& expr = ast["select"]["items"][0]["expression"];
        REQUIRE(expr["type"] == "Identifier");
    }

    SECTION("Without optimization") {
        auto result = parser.parse("SELECT 1 + 1 FROM t", false);
        REQUIRE(result.has_value());
        REQUIRE(result->optimized == false);

        auto& ast = result->ast;
        auto& expr = ast["select"]["items"][0]["expression"];
        REQUIRE(expr["type"] == "BinaryExpr");
    }
}

TEST_CASE("Full pipeline - JSON output", "[integration]") {
    SQLParser parser;

    SECTION("JSON string output") {
        std::string json = parser.parseToJson("SELECT 1");
        REQUIRE(!json.empty());
        REQUIRE(json.find("SelectStatement") != std::string::npos);
    }

    SECTION("Error JSON output") {
        std::string json = parser.parseToJson("SELECT");
        REQUIRE(json.find("error") != std::string::npos);
        REQUIRE(json.find("true") != std::string::npos);
    }
}

TEST_CASE("Full pipeline - complex queries", "[integration]") {
    SQLParser parser;

    SECTION("Multiple JOINs") {
        auto result = parser.parse(
            "SELECT o.id, c.name, p.title "
            "FROM orders o "
            "JOIN customers c ON o.customer_id = c.id "
            "LEFT JOIN products p ON o.product_id = p.id "
            "WHERE o.total > 100"
        );
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast["from"]["joins"].size() == 2);
    }

    SECTION("GROUP BY with HAVING") {
        auto result = parser.parse(
            "SELECT category, COUNT(*) as cnt "
            "FROM products "
            "GROUP BY category "
            "HAVING COUNT(*) > 5"
        );
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast.contains("groupBy"));
        REQUIRE(ast.contains("having"));
    }

    SECTION("Subquery in WHERE") {
        auto result = parser.parse(
            "SELECT name FROM users WHERE id IN (SELECT user_id FROM admins)"
        );
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast["where"]["type"] == "InExpr");
    }

    SECTION("UNION") {
        auto result = parser.parse(
            "SELECT name FROM users UNION SELECT name FROM admins"
        );
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast["setOperation"] == "UNION");
    }

    SECTION("ORDER BY with LIMIT") {
        auto result = parser.parse(
            "SELECT name, score FROM players ORDER BY score DESC LIMIT 10 OFFSET 5"
        );
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast.contains("orderBy"));
        REQUIRE(ast["orderBy"][0]["ascending"] == false);
        REQUIRE(ast.contains("limit"));
    }

    SECTION("CASE expression") {
        auto result = parser.parse(
            "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM users"
        );
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        auto& expr = ast["select"]["items"][0]["expression"];
        REQUIRE(expr["type"] == "CaseExpr");
    }

    SECTION("CAST expression") {
        auto result = parser.parse(
            "SELECT CAST(price AS INTEGER) FROM products"
        );
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        auto& expr = ast["select"]["items"][0]["expression"];
        REQUIRE(expr["type"] == "CastExpr");
        REQUIRE(expr["targetType"] == "INTEGER");
    }

    SECTION("BETWEEN expression") {
        auto result = parser.parse(
            "SELECT id FROM events WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'"
        );
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast["where"]["type"] == "BetweenExpr");
    }

    SECTION("LIKE expression") {
        auto result = parser.parse(
            "SELECT * FROM users WHERE name LIKE '%john%'"
        );
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast["where"]["type"] == "LikeExpr");
    }

    SECTION("EXISTS expression") {
        auto result = parser.parse(
            "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
        );
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast["where"]["type"] == "ExistsExpr");
    }
}

TEST_CASE("Full pipeline - aggregations", "[integration]") {
    SQLParser parser;

    SECTION("COUNT DISTINCT") {
        auto result = parser.parse("SELECT COUNT(DISTINCT category) FROM products");
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        auto& func = ast["select"]["items"][0]["expression"];
        REQUIRE(func["type"] == "FunctionCall");
        REQUIRE(func["name"] == "COUNT");
        REQUIRE(func["distinct"] == true);
    }

    SECTION("Multiple aggregations") {
        auto result = parser.parse(
            "SELECT MIN(price), MAX(price), AVG(price), SUM(quantity) FROM products"
        );
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast["select"]["items"].size() == 4);
    }
}

TEST_CASE("Full pipeline - error handling", "[integration]") {
    SQLParser parser;

    SECTION("Missing FROM clause after comma") {
        auto result = parser.parse("SELECT a, FROM t");
        REQUIRE(!result.has_value());
    }

    SECTION("Double comma in SELECT") {
        auto result = parser.parse("SELECT a,, b FROM t");
        REQUIRE(!result.has_value());
    }

    SECTION("Invalid keyword in expression") {
        auto result = parser.parse("SELECT FROM t");
        REQUIRE(!result.has_value());
    }
}

TEST_CASE("Full pipeline - aliases", "[integration]") {
    SQLParser parser;

    SECTION("Column alias") {
        auto result = parser.parse("SELECT name AS user_name FROM users");
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast["select"]["items"][0]["alias"] == "user_name");
    }

    SECTION("Table alias") {
        auto result = parser.parse("SELECT u.name FROM users u");
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast["from"]["primary"]["alias"] == "u");
    }

    SECTION("Subquery alias") {
        auto result = parser.parse(
            "SELECT sq.total FROM (SELECT SUM(amount) as total FROM orders) sq"
        );
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        REQUIRE(ast["from"]["primary"]["type"] == "SubqueryTable");
        REQUIRE(ast["from"]["primary"]["alias"] == "sq");
    }
}

TEST_CASE("Full pipeline - edge cases", "[integration]") {
    SQLParser parser;

    SECTION("Empty string") {
        auto result = parser.parse("");
        REQUIRE(!result.has_value());
    }

    SECTION("Whitespace only") {
        auto result = parser.parse("   \n\t  ");
        REQUIRE(!result.has_value());
    }

    SECTION("Very long column list") {
        std::string sql = "SELECT a";
        for (int i = 0; i < 50; ++i) {
            sql += ", col" + std::to_string(i);
        }
        sql += " FROM t";

        auto result = parser.parse(sql);
        REQUIRE(result.has_value());
        REQUIRE(result->ast["select"]["items"].size() == 51);
    }

    SECTION("Nested subqueries") {
        auto result = parser.parse(
            "SELECT * FROM (SELECT * FROM (SELECT * FROM t) sub1) sub2"
        );
        REQUIRE(result.has_value());
    }

    SECTION("Complex nested expression") {
        auto result = parser.parse(
            "SELECT ((a + b) * (c - d)) / (e + 1) FROM t"
        );
        REQUIRE(result.has_value());
    }

    SECTION("String with escaped quotes") {
        auto result = parser.parse("SELECT 'it''s a test' FROM t");
        REQUIRE(result.has_value());

        auto& ast = result->ast;
        auto& lit = ast["select"]["items"][0]["expression"];
        REQUIRE(lit["value"] == "it's a test");
    }
}
