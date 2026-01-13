#include "Lexer.hpp"
#include <algorithm>
#include <cctype>

const std::unordered_map<std::string, KeywordType> Lexer::keywords_ = {
    {"SELECT", KeywordType::SELECT},
    {"FROM", KeywordType::FROM},
    {"WHERE", KeywordType::WHERE},
    {"BETWEEN", KeywordType::BETWEEN},
    {"DISTINCT", KeywordType::DISTINCT},
    {"LIKE", KeywordType::LIKE},
    {"IN", KeywordType::IN},
    {"GROUP", KeywordType::GROUP},
    {"BY", KeywordType::BY},
    {"ORDER", KeywordType::ORDER},
    {"HAVING", KeywordType::HAVING},
    {"ASC", KeywordType::ASC},
    {"DESC", KeywordType::DESC},
    {"JOIN", KeywordType::JOIN},
    {"INNER", KeywordType::INNER},
    {"LEFT", KeywordType::LEFT},
    {"RIGHT", KeywordType::RIGHT},
    {"FULL", KeywordType::FULL},
    {"OUTER", KeywordType::OUTER},
    {"CROSS", KeywordType::CROSS},
    {"ON", KeywordType::ON},
    {"USING", KeywordType::USING},
    {"AND", KeywordType::AND},
    {"OR", KeywordType::OR},
    {"NOT", KeywordType::NOT},
    {"LIMIT", KeywordType::LIMIT},
    {"OFFSET", KeywordType::OFFSET},
    {"TOP", KeywordType::TOP},
    {"FETCH", KeywordType::FETCH},
    {"MIN", KeywordType::MIN},
    {"MAX", KeywordType::MAX},
    {"COUNT", KeywordType::COUNT},
    {"SUM", KeywordType::SUM},
    {"AVG", KeywordType::AVG},
    {"INT", KeywordType::INT},
    {"INTEGER", KeywordType::INTEGER},
    {"BIGINT", KeywordType::BIGINT},
    {"FLOAT", KeywordType::FLOAT},
    {"DOUBLE", KeywordType::DOUBLE},
    {"DECIMAL", KeywordType::DECIMAL},
    {"TEXT", KeywordType::TEXT},
    {"BOOLEAN", KeywordType::BOOLEAN},
    {"DATE", KeywordType::DATE},
    {"TIME", KeywordType::TIME},
    {"TIMESTAMP", KeywordType::TIMESTAMP},
    {"UNION", KeywordType::UNION},
    {"INTERSECT", KeywordType::INTERSECT},
    {"EXCEPT", KeywordType::EXCEPT},
    {"MINUS", KeywordType::MINUS},
    {"CASE", KeywordType::CASE},
    {"WHEN", KeywordType::WHEN},
    {"THEN", KeywordType::THEN},
    {"ELSE", KeywordType::ELSE},
    {"END", KeywordType::END},
    {"AS", KeywordType::AS},
    {"ALL", KeywordType::ALL},
    {"ANY", KeywordType::ANY},
    {"SOME", KeywordType::SOME},
    {"EXISTS", KeywordType::EXISTS},
    {"NULL", KeywordType::NULL_KW},
    {"CAST", KeywordType::CAST},
    {"IS", KeywordType::IS},
    {"TRUE", KeywordType::TRUE_KW},
    {"FALSE", KeywordType::FALSE_KW},
};

Lexer::Lexer(std::string_view source) : source_(source) {}

std::expected<std::vector<Token>, ParseError> Lexer::tokenize() {
    std::vector<Token> tokens;
    while (!isAtEnd()) {
        auto token = nextToken();
        if (!token) {
            return std::unexpected(token.error());
        }
        tokens.push_back(std::move(*token));
        if (tokens.back().type == TokenType::EndOfFile) {
            break;
        }
    }
    if (tokens.empty() || tokens.back().type != TokenType::EndOfFile) {
        tokens.push_back(makeToken(TokenType::EndOfFile));
    }
    return tokens;
}

std::expected<Token, ParseError> Lexer::nextToken() {
    skipWhitespace();
    start_ = current_;
    if (isAtEnd()) {
        return makeToken(TokenType::EndOfFile);
    }
    return scanToken();
}

bool Lexer::isAtEnd() const noexcept {
    return current_ >= source_.size();
}

char Lexer::advance() {
    char c = source_[current_++];
    if (c == '\n') {
        line_++;
        column_ = 1;
    } else {
        column_++;
    }
    return c;
}

char Lexer::peek() const {
    if (isAtEnd()) return '\0';
    return source_[current_];
}

char Lexer::peekNext() const {
    if (current_ + 1 >= source_.size()) return '\0';
    return source_[current_ + 1];
}

bool Lexer::match(char expected) {
    if (isAtEnd() || source_[current_] != expected) return false;
    advance();
    return true;
}

void Lexer::skipWhitespace() {
    while (!isAtEnd()) {
        char c = peek();
        switch (c) {
            case ' ':
            case '\t':
            case '\r':
            case '\n':
                advance();
                break;
            case '-':
                if (peekNext() == '-') {
                    skipLineComment();
                } else {
                    return;
                }
                break;
            case '/':
                if (peekNext() == '*') {
                    skipBlockComment();
                } else {
                    return;
                }
                break;
            default:
                return;
        }
    }
}

void Lexer::skipLineComment() {
    while (!isAtEnd() && peek() != '\n') {
        advance();
    }
}

void Lexer::skipBlockComment() {
    advance(); // /
    advance(); // *
    while (!isAtEnd()) {
        if (peek() == '*' && peekNext() == '/') {
            advance();
            advance();
            return;
        }
        advance();
    }
}

std::expected<Token, ParseError> Lexer::scanToken() {
    char c = peek();

    if (isAlpha(c) || c == '_') {
        return scanIdentifierOrKeyword();
    }

    if (isDigit(c)) {
        return scanNumber();
    }

    if (c == '\'') {
        return scanString();
    }

    if (c == '"') {
        advance();
        size_t startPos = current_;
        while (!isAtEnd() && peek() != '"') {
            advance();
        }
        if (isAtEnd()) {
            return std::unexpected(makeError("Unterminated quoted identifier"));
        }
        std::string identifier(source_.substr(startPos, current_ - startPos));
        advance();
        Token token = makeToken(TokenType::Identifier);
        token.text = std::move(identifier);
        return token;
    }

    switch (c) {
        case '(':
            advance();
            return makeDelimiterToken(DelimiterType::LeftParen);
        case ')':
            advance();
            return makeDelimiterToken(DelimiterType::RightParen);
        case ',':
            advance();
            return makeDelimiterToken(DelimiterType::Comma);
        case ';':
            advance();
            return makeDelimiterToken(DelimiterType::Semicolon);
        case '.':
            advance();
            return makeDelimiterToken(DelimiterType::Dot);
        case '*':
            advance();
            return makeDelimiterToken(DelimiterType::Star);
        case '+':
        case '-':
        case '/':
        case '%':
        case '=':
        case '<':
        case '>':
        case '!':
        case '|':
            return scanOperator();
        default:
            advance();
            return std::unexpected(makeError("Unexpected character: " + std::string(1, c)));
    }
}

Token Lexer::scanIdentifierOrKeyword() {
    while (!isAtEnd() && (isAlphaNumeric(peek()) || peek() == '_')) {
        advance();
    }
    std::string text(source_.substr(start_, current_ - start_));
    std::string upper = text;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);

    auto it = keywords_.find(upper);
    if (it != keywords_.end()) {
        return makeKeywordToken(it->second);
    }
    Token token = makeToken(TokenType::Identifier);
    token.text = std::move(text);
    return token;
}

std::expected<Token, ParseError> Lexer::scanNumber() {
    while (!isAtEnd() && isDigit(peek())) {
        advance();
    }

    bool isFloat = false;
    if (peek() == '.' && isDigit(peekNext())) {
        isFloat = true;
        advance();
        while (!isAtEnd() && isDigit(peek())) {
            advance();
        }
    }

    std::string numStr(source_.substr(start_, current_ - start_));
    if (isFloat) {
        try {
            double value = std::stod(numStr);
            return makeLiteralToken(value);
        } catch (...) {
            return std::unexpected(makeError("Invalid floating point number: " + numStr));
        }
    } else {
        try {
            long value = std::stol(numStr);
            return makeLiteralToken(value);
        } catch (...) {
            return std::unexpected(makeError("Invalid integer: " + numStr));
        }
    }
}

std::expected<Token, ParseError> Lexer::scanString() {
    advance(); // opening quote
    std::string value;
    while (!isAtEnd()) {
        char c = peek();
        if (c == '\'') {
            if (peekNext() == '\'') {
                value += '\'';
                advance();
                advance();
            } else {
                break;
            }
        } else {
            value += c;
            advance();
        }
    }

    if (isAtEnd()) {
        return std::unexpected(makeError("Unterminated string literal"));
    }
    advance(); // closing quote
    return makeLiteralToken(std::move(value));
}

std::expected<Token, ParseError> Lexer::scanOperator() {
    char c = advance();
    switch (c) {
        case '+':
            return makeOperatorToken(OperatorType::Plus);
        case '-':
            return makeOperatorToken(OperatorType::Minus);
        case '/':
            return makeOperatorToken(OperatorType::Divide);
        case '%':
            return makeOperatorToken(OperatorType::Modulo);
        case '=':
            return makeOperatorToken(OperatorType::Equal);
        case '<':
            if (match('=')) return makeOperatorToken(OperatorType::LessEqual);
            if (match('>')) return makeOperatorToken(OperatorType::NotEqual);
            return makeOperatorToken(OperatorType::LessThan);
        case '>':
            if (match('=')) return makeOperatorToken(OperatorType::GreaterEqual);
            return makeOperatorToken(OperatorType::GreaterThan);
        case '!':
            if (match('=')) return makeOperatorToken(OperatorType::NotEqual);
            return std::unexpected(makeError("Expected '=' after '!'"));
        case '|':
            if (match('|')) return makeOperatorToken(OperatorType::Concat);
            return std::unexpected(makeError("Expected '|' after '|'"));
        default:
            return std::unexpected(makeError("Unknown operator"));
    }
}

Token Lexer::scanDelimiter() {
    char c = advance();
    switch (c) {
        case '(':
            return makeDelimiterToken(DelimiterType::LeftParen);
        case ')':
            return makeDelimiterToken(DelimiterType::RightParen);
        case ',':
            return makeDelimiterToken(DelimiterType::Comma);
        case ';':
            return makeDelimiterToken(DelimiterType::Semicolon);
        case '.':
            return makeDelimiterToken(DelimiterType::Dot);
        case '*':
            return makeDelimiterToken(DelimiterType::Star);
        default:
            return makeToken(TokenType::Delimiter);
    }
}

bool Lexer::isDigit(char c) {
    return c >= '0' && c <= '9';
}

bool Lexer::isAlpha(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

bool Lexer::isAlphaNumeric(char c) {
    return isAlpha(c) || isDigit(c);
}

Token Lexer::makeToken(TokenType type) const {
    Token token;
    token.text = std::string(source_.substr(start_, current_ - start_));
    token.type = type;
    token.line = line_;
    token.column = column_ - (current_ - start_);
    return token;
}

Token Lexer::makeKeywordToken(KeywordType kw) const {
    Token token = makeToken(TokenType::Keyword);
    token.keywordType = kw;
    return token;
}

Token Lexer::makeOperatorToken(OperatorType op) const {
    Token token = makeToken(TokenType::Operator);
    token.operatorType = op;
    return token;
}

Token Lexer::makeDelimiterToken(DelimiterType delim) const {
    Token token = makeToken(TokenType::Delimiter);
    token.delimiterType = delim;
    return token;
}

Token Lexer::makeLiteralToken(SqlLiteral value) const {
    Token token = makeToken(TokenType::Literal);
    token.literalValue = std::move(value);
    return token;
}

ParseError Lexer::makeError(std::string message) const {
    ParseError error;
    error.message = std::move(message);
    error.location = {line_, column_};
    size_t lineStart = start_;
    while (lineStart > 0 && source_[lineStart - 1] != '\n') {
        lineStart--;
    }
    size_t lineEnd = current_;
    while (lineEnd < source_.size() && source_[lineEnd] != '\n') {
        lineEnd++;
    }
    error.snippet = std::string(source_.substr(lineStart, lineEnd - lineStart));
    return error;
}
