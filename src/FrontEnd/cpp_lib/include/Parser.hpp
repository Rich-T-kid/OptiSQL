#pragma once
#include "AST.hpp"
#include "Error.hpp"
#include "Token.hpp"
#include <expected>
#include <vector>

class Parser {
public:
    explicit Parser(std::vector<Token> tokens);

    std::expected<StmtPtr, ParseError> parse();

private:
    std::vector<Token> tokens_;
    size_t current_{0};

    std::expected<StmtPtr, ParseError> parseSelectStatement();
    std::expected<SelectClause, ParseError> parseSelectClause();
    std::expected<SelectItem, ParseError> parseSelectItem();
    std::expected<FromClause, ParseError> parseFromClause();
    std::expected<TableRef, ParseError> parseTableRef();
    std::expected<JoinClause, ParseError> parseJoinClause();
    std::expected<WhereClause, ParseError> parseWhereClause();
    std::expected<GroupByClause, ParseError> parseGroupByClause();
    std::expected<HavingClause, ParseError> parseHavingClause();
    std::expected<OrderByClause, ParseError> parseOrderByClause();
    std::expected<LimitClause, ParseError> parseLimitClause();

    std::expected<ExprPtr, ParseError> parseExpression();
    std::expected<ExprPtr, ParseError> parseOr();
    std::expected<ExprPtr, ParseError> parseAnd();
    std::expected<ExprPtr, ParseError> parseNot();
    std::expected<ExprPtr, ParseError> parseComparison();
    std::expected<ExprPtr, ParseError> parseAdditive();
    std::expected<ExprPtr, ParseError> parseMultiplicative();
    std::expected<ExprPtr, ParseError> parseUnary();
    std::expected<ExprPtr, ParseError> parsePrimary();
    std::expected<ExprPtr, ParseError> parseFunctionCall(const std::string& name);
    std::expected<ExprPtr, ParseError> parseCaseExpression();
    std::expected<ExprPtr, ParseError> parseCastExpression();

    [[nodiscard]] const Token& peek() const;
    [[nodiscard]] const Token& previous() const;
    Token advance();
    [[nodiscard]] bool check(TokenType type) const;
    [[nodiscard]] bool checkKeyword(KeywordType kw) const;
    [[nodiscard]] bool checkDelimiter(DelimiterType d) const;
    bool match(TokenType type);
    bool matchKeyword(KeywordType kw);
    bool matchDelimiter(DelimiterType d);
    std::expected<Token, ParseError> consume(TokenType type, std::string_view message);
    std::expected<Token, ParseError> consumeKeyword(KeywordType kw, std::string_view message);
    std::expected<Token, ParseError> consumeDelimiter(DelimiterType d, std::string_view message);
    [[nodiscard]] bool isAtEnd() const;

    ParseError error(const Token& token, std::string message) const;
};
