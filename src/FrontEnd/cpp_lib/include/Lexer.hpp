#pragma once
#include "Token.hpp"
#include "Error.hpp"
#include <expected>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

class Lexer {
public:
    explicit Lexer(std::string_view source);

    std::expected<std::vector<Token>, ParseError> tokenize();
    std::expected<Token, ParseError> nextToken();
    [[nodiscard]] bool isAtEnd() const noexcept;

private:
    std::string_view source_;
    size_t current_{0};
    size_t start_{0};
    size_t line_{1};
    size_t column_{1};

    char advance();
    [[nodiscard]] char peek() const;
    [[nodiscard]] char peekNext() const;
    bool match(char expected);
    void skipWhitespace();
    void skipLineComment();
    void skipBlockComment();

    std::expected<Token, ParseError> scanToken();
    Token scanIdentifierOrKeyword();
    std::expected<Token, ParseError> scanNumber();
    std::expected<Token, ParseError> scanString();
    std::expected<Token, ParseError> scanOperator();
    Token scanDelimiter();

    [[nodiscard]] static bool isDigit(char c);
    [[nodiscard]] static bool isAlpha(char c);
    [[nodiscard]] static bool isAlphaNumeric(char c);

    Token makeToken(TokenType type) const;
    Token makeKeywordToken(KeywordType kw) const;
    Token makeOperatorToken(OperatorType op) const;
    Token makeDelimiterToken(DelimiterType delim) const;
    Token makeLiteralToken(SqlLiteral value) const;
    ParseError makeError(std::string message) const;

    static const std::unordered_map<std::string, KeywordType> keywords_;
};
