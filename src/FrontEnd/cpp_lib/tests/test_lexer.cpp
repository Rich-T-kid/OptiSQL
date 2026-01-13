#include <catch2/catch_amalgamated.hpp>
#include "Lexer.hpp"

TEST_CASE("Lexer tokenizes simple SELECT", "[lexer]") {
    Lexer lexer("SELECT * FROM t");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    REQUIRE(tokens.size() == 5);
    CHECK(tokens[0].type == TokenType::Keyword);
    CHECK(tokens[0].keywordType == KeywordType::SELECT);
    CHECK(tokens[1].type == TokenType::Delimiter);
    CHECK(tokens[1].delimiterType == DelimiterType::Star);
    CHECK(tokens[2].type == TokenType::Keyword);
    CHECK(tokens[2].keywordType == KeywordType::FROM);
    CHECK(tokens[3].type == TokenType::Identifier);
    CHECK(tokens[3].text == "t");
    CHECK(tokens[4].type == TokenType::EndOfFile);
}

TEST_CASE("Lexer handles multiple columns", "[lexer]") {
    Lexer lexer("SELECT a, b, c FROM table1");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    REQUIRE(tokens.size() == 9);
    CHECK(tokens[1].type == TokenType::Identifier);
    CHECK(tokens[1].text == "a");
    CHECK(tokens[2].type == TokenType::Delimiter);
    CHECK(tokens[2].delimiterType == DelimiterType::Comma);
}

TEST_CASE("Lexer handles integer literals", "[lexer]") {
    Lexer lexer("SELECT 0, 1, 999999999");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    CHECK(std::get<long>(*tokens[1].literalValue) == 0);
    CHECK(std::get<long>(*tokens[3].literalValue) == 1);
    CHECK(std::get<long>(*tokens[5].literalValue) == 999999999);
}

TEST_CASE("Lexer handles float literals", "[lexer]") {
    Lexer lexer("SELECT 3.14, 0.001, 123.456");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    CHECK(std::get<double>(*tokens[1].literalValue) == Catch::Approx(3.14));
    CHECK(std::get<double>(*tokens[3].literalValue) == Catch::Approx(0.001));
    CHECK(std::get<double>(*tokens[5].literalValue) == Catch::Approx(123.456));
}

TEST_CASE("Lexer handles string literals", "[lexer]") {
    Lexer lexer("SELECT 'hello', 'world''s'");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    CHECK(std::get<std::string>(*tokens[1].literalValue) == "hello");
    CHECK(std::get<std::string>(*tokens[3].literalValue) == "world's");
}

TEST_CASE("Lexer handles empty string", "[lexer]") {
    Lexer lexer("SELECT ''");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    CHECK(std::get<std::string>(*(*result)[1].literalValue) == "");
}

TEST_CASE("Lexer handles identifiers with underscores", "[lexer]") {
    Lexer lexer("SELECT _underscore, __double, col123");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    CHECK(tokens[1].text == "_underscore");
    CHECK(tokens[3].text == "__double");
    CHECK(tokens[5].text == "col123");
}

TEST_CASE("Lexer handles quoted identifiers", "[lexer]") {
    Lexer lexer("SELECT \"quoted identifier\"");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    CHECK((*result)[1].type == TokenType::Identifier);
    CHECK((*result)[1].text == "quoted identifier");
}

TEST_CASE("Lexer handles arithmetic operators", "[lexer]") {
    Lexer lexer("SELECT 1+2, 3-4, 7/8, 9%10");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    CHECK(tokens[2].operatorType == OperatorType::Plus);
    CHECK(tokens[6].operatorType == OperatorType::Minus);
    CHECK(tokens[10].operatorType == OperatorType::Divide);
    CHECK(tokens[14].operatorType == OperatorType::Modulo);
}

TEST_CASE("Lexer handles star as delimiter", "[lexer]") {
    Lexer lexer("SELECT 5*6");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    CHECK(tokens[2].type == TokenType::Delimiter);
    CHECK(tokens[2].delimiterType == DelimiterType::Star);
}

TEST_CASE("Lexer handles comparison operators", "[lexer]") {
    Lexer lexer("SELECT a=b, c<>d, e<f, g<=h, i>j, k>=l");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    CHECK(tokens[2].operatorType == OperatorType::Equal);
    CHECK(tokens[6].operatorType == OperatorType::NotEqual);
    CHECK(tokens[10].operatorType == OperatorType::LessThan);
    CHECK(tokens[14].operatorType == OperatorType::LessEqual);
    CHECK(tokens[18].operatorType == OperatorType::GreaterThan);
    CHECK(tokens[22].operatorType == OperatorType::GreaterEqual);
}

TEST_CASE("Lexer handles != operator", "[lexer]") {
    Lexer lexer("SELECT a != b");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    CHECK((*result)[2].operatorType == OperatorType::NotEqual);
}

TEST_CASE("Lexer handles concat operator", "[lexer]") {
    Lexer lexer("SELECT a || b");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    CHECK((*result)[2].operatorType == OperatorType::Concat);
}

TEST_CASE("Lexer handles line comments", "[lexer]") {
    Lexer lexer("SELECT 1 -- this is a comment\nFROM t");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    REQUIRE(tokens.size() == 5);
    CHECK(tokens[0].keywordType == KeywordType::SELECT);
    CHECK(std::get<long>(*tokens[1].literalValue) == 1);
    CHECK(tokens[2].keywordType == KeywordType::FROM);
}

TEST_CASE("Lexer handles block comments", "[lexer]") {
    Lexer lexer("SELECT /* block */ 2");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    REQUIRE(tokens.size() == 3);
    CHECK(std::get<long>(*tokens[1].literalValue) == 2);
}

TEST_CASE("Lexer handles multi-line block comments", "[lexer]") {
    Lexer lexer("SELECT /* multi\nline\ncomment */ 3");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    CHECK(std::get<long>(*(*result)[1].literalValue) == 3);
}

TEST_CASE("Lexer handles all keywords", "[lexer]") {
    SECTION("DML keywords") {
        Lexer lexer("SELECT FROM WHERE");
        auto result = lexer.tokenize();
        REQUIRE(result.has_value());
        CHECK((*result)[0].keywordType == KeywordType::SELECT);
        CHECK((*result)[1].keywordType == KeywordType::FROM);
        CHECK((*result)[2].keywordType == KeywordType::WHERE);
    }

    SECTION("Filtering keywords") {
        Lexer lexer("BETWEEN DISTINCT LIKE IN");
        auto result = lexer.tokenize();
        REQUIRE(result.has_value());
        CHECK((*result)[0].keywordType == KeywordType::BETWEEN);
        CHECK((*result)[1].keywordType == KeywordType::DISTINCT);
        CHECK((*result)[2].keywordType == KeywordType::LIKE);
        CHECK((*result)[3].keywordType == KeywordType::IN);
    }

    SECTION("Grouping keywords") {
        Lexer lexer("GROUP BY ORDER HAVING ASC DESC");
        auto result = lexer.tokenize();
        REQUIRE(result.has_value());
        CHECK((*result)[0].keywordType == KeywordType::GROUP);
        CHECK((*result)[1].keywordType == KeywordType::BY);
        CHECK((*result)[2].keywordType == KeywordType::ORDER);
        CHECK((*result)[3].keywordType == KeywordType::HAVING);
        CHECK((*result)[4].keywordType == KeywordType::ASC);
        CHECK((*result)[5].keywordType == KeywordType::DESC);
    }

    SECTION("Join keywords") {
        Lexer lexer("JOIN INNER LEFT RIGHT FULL OUTER CROSS ON USING");
        auto result = lexer.tokenize();
        REQUIRE(result.has_value());
        CHECK((*result)[0].keywordType == KeywordType::JOIN);
        CHECK((*result)[1].keywordType == KeywordType::INNER);
        CHECK((*result)[2].keywordType == KeywordType::LEFT);
        CHECK((*result)[3].keywordType == KeywordType::RIGHT);
        CHECK((*result)[4].keywordType == KeywordType::FULL);
        CHECK((*result)[5].keywordType == KeywordType::OUTER);
        CHECK((*result)[6].keywordType == KeywordType::CROSS);
        CHECK((*result)[7].keywordType == KeywordType::ON);
        CHECK((*result)[8].keywordType == KeywordType::USING);
    }

    SECTION("Logical keywords") {
        Lexer lexer("AND OR NOT");
        auto result = lexer.tokenize();
        REQUIRE(result.has_value());
        CHECK((*result)[0].keywordType == KeywordType::AND);
        CHECK((*result)[1].keywordType == KeywordType::OR);
        CHECK((*result)[2].keywordType == KeywordType::NOT);
    }

    SECTION("Aggregate keywords") {
        Lexer lexer("MIN MAX COUNT SUM AVG");
        auto result = lexer.tokenize();
        REQUIRE(result.has_value());
        CHECK((*result)[0].keywordType == KeywordType::MIN);
        CHECK((*result)[1].keywordType == KeywordType::MAX);
        CHECK((*result)[2].keywordType == KeywordType::COUNT);
        CHECK((*result)[3].keywordType == KeywordType::SUM);
        CHECK((*result)[4].keywordType == KeywordType::AVG);
    }

    SECTION("Type keywords") {
        Lexer lexer("INT INTEGER BIGINT FLOAT DOUBLE DECIMAL TEXT BOOLEAN DATE TIME TIMESTAMP");
        auto result = lexer.tokenize();
        REQUIRE(result.has_value());
        CHECK((*result)[0].keywordType == KeywordType::INT);
        CHECK((*result)[1].keywordType == KeywordType::INTEGER);
        CHECK((*result)[2].keywordType == KeywordType::BIGINT);
        CHECK((*result)[3].keywordType == KeywordType::FLOAT);
        CHECK((*result)[4].keywordType == KeywordType::DOUBLE);
        CHECK((*result)[5].keywordType == KeywordType::DECIMAL);
        CHECK((*result)[6].keywordType == KeywordType::TEXT);
        CHECK((*result)[7].keywordType == KeywordType::BOOLEAN);
        CHECK((*result)[8].keywordType == KeywordType::DATE);
        CHECK((*result)[9].keywordType == KeywordType::TIME);
        CHECK((*result)[10].keywordType == KeywordType::TIMESTAMP);
    }

    SECTION("Set operation keywords") {
        Lexer lexer("UNION INTERSECT EXCEPT MINUS");
        auto result = lexer.tokenize();
        REQUIRE(result.has_value());
        CHECK((*result)[0].keywordType == KeywordType::UNION);
        CHECK((*result)[1].keywordType == KeywordType::INTERSECT);
        CHECK((*result)[2].keywordType == KeywordType::EXCEPT);
        CHECK((*result)[3].keywordType == KeywordType::MINUS);
    }

    SECTION("Case keywords") {
        Lexer lexer("CASE WHEN THEN ELSE END");
        auto result = lexer.tokenize();
        REQUIRE(result.has_value());
        CHECK((*result)[0].keywordType == KeywordType::CASE);
        CHECK((*result)[1].keywordType == KeywordType::WHEN);
        CHECK((*result)[2].keywordType == KeywordType::THEN);
        CHECK((*result)[3].keywordType == KeywordType::ELSE);
        CHECK((*result)[4].keywordType == KeywordType::END);
    }

    SECTION("Other keywords") {
        Lexer lexer("AS ALL ANY SOME EXISTS NULL CAST IS TRUE FALSE");
        auto result = lexer.tokenize();
        REQUIRE(result.has_value());
        CHECK((*result)[0].keywordType == KeywordType::AS);
        CHECK((*result)[1].keywordType == KeywordType::ALL);
        CHECK((*result)[2].keywordType == KeywordType::ANY);
        CHECK((*result)[3].keywordType == KeywordType::SOME);
        CHECK((*result)[4].keywordType == KeywordType::EXISTS);
        CHECK((*result)[5].keywordType == KeywordType::NULL_KW);
        CHECK((*result)[6].keywordType == KeywordType::CAST);
        CHECK((*result)[7].keywordType == KeywordType::IS);
        CHECK((*result)[8].keywordType == KeywordType::TRUE_KW);
        CHECK((*result)[9].keywordType == KeywordType::FALSE_KW);
    }
}

TEST_CASE("Lexer handles delimiters", "[lexer]") {
    Lexer lexer("( ) , ; . *");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    CHECK(tokens[0].delimiterType == DelimiterType::LeftParen);
    CHECK(tokens[1].delimiterType == DelimiterType::RightParen);
    CHECK(tokens[2].delimiterType == DelimiterType::Comma);
    CHECK(tokens[3].delimiterType == DelimiterType::Semicolon);
    CHECK(tokens[4].delimiterType == DelimiterType::Dot);
    CHECK(tokens[5].delimiterType == DelimiterType::Star);
}

TEST_CASE("Lexer case insensitivity for keywords", "[lexer]") {
    Lexer lexer("select SELECT Select sElEcT");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    for (size_t i = 0; i < 4; i++) {
        CHECK(tokens[i].keywordType == KeywordType::SELECT);
    }
}

TEST_CASE("Lexer tracks line numbers", "[lexer]") {
    Lexer lexer("SELECT\na\nFROM\nt");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    CHECK(tokens[0].line == 1);
    CHECK(tokens[1].line == 2);
    CHECK(tokens[2].line == 3);
    CHECK(tokens[3].line == 4);
}

TEST_CASE("Lexer reports unterminated string", "[lexer]") {
    Lexer lexer("SELECT 'unterminated");
    auto result = lexer.tokenize();

    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().message.find("Unterminated") != std::string::npos);
}

TEST_CASE("Lexer reports unterminated quoted identifier", "[lexer]") {
    Lexer lexer("SELECT \"unterminated");
    auto result = lexer.tokenize();

    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().message.find("Unterminated") != std::string::npos);
}

TEST_CASE("Lexer reports unexpected character", "[lexer]") {
    Lexer lexer("SELECT @invalid");
    auto result = lexer.tokenize();

    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().message.find("Unexpected") != std::string::npos);
}

TEST_CASE("Lexer handles complex query", "[lexer]") {
    Lexer lexer(R"(
        SELECT u.name, COUNT(*) as cnt
        FROM users u
        INNER JOIN orders o ON u.id = o.user_id
        WHERE o.total > 100.50
        GROUP BY u.name
        HAVING COUNT(*) >= 5
        ORDER BY cnt DESC
        LIMIT 10
    )");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    CHECK(result->size() > 30);
}

TEST_CASE("Lexer handles deeply nested parens", "[lexer]") {
    Lexer lexer("SELECT ((((((((((1))))))))))");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    auto& tokens = *result;

    int parenCount = 0;
    for (const auto& token : tokens) {
        if (token.delimiterType == DelimiterType::LeftParen) parenCount++;
        if (token.delimiterType == DelimiterType::RightParen) parenCount--;
    }
    CHECK(parenCount == 0);
}

TEST_CASE("Lexer handles very small float", "[lexer]") {
    Lexer lexer("SELECT 0.0000000001");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    CHECK(std::get<double>(*(*result)[1].literalValue) == Catch::Approx(0.0000000001));
}

TEST_CASE("Lexer handles multiline string", "[lexer]") {
    Lexer lexer("SELECT 'line1\nline2'");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    CHECK(std::get<std::string>(*(*result)[1].literalValue) == "line1\nline2");
}

TEST_CASE("Lexer handles escaped quote in string", "[lexer]") {
    Lexer lexer("SELECT ''''");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    CHECK(std::get<std::string>(*(*result)[1].literalValue) == "'");
}

TEST_CASE("Lexer handles limit and offset keywords", "[lexer]") {
    Lexer lexer("LIMIT OFFSET TOP FETCH");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    CHECK((*result)[0].keywordType == KeywordType::LIMIT);
    CHECK((*result)[1].keywordType == KeywordType::OFFSET);
    CHECK((*result)[2].keywordType == KeywordType::TOP);
    CHECK((*result)[3].keywordType == KeywordType::FETCH);
}

TEST_CASE("Lexer handles empty input", "[lexer]") {
    Lexer lexer("");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    REQUIRE(result->size() == 1);
    CHECK((*result)[0].type == TokenType::EndOfFile);
}

TEST_CASE("Lexer handles whitespace only", "[lexer]") {
    Lexer lexer("   \t\n\r  ");
    auto result = lexer.tokenize();

    REQUIRE(result.has_value());
    REQUIRE(result->size() == 1);
    CHECK((*result)[0].type == TokenType::EndOfFile);
}
