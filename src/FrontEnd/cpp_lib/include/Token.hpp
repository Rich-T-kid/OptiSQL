#pragma once
#include <cstddef>
#include <optional>
#include <string>
#include <variant>
#include <vector>

struct Null {};

using SqlLiteral = std::variant<std::string, long, double, bool, Null>;

enum class TokenType {
    Keyword,
    Identifier,
    Literal,
    Operator,
    Delimiter,
    EndOfFile
};

enum class OperatorType {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Equal,
    NotEqual,
    LessThan,
    LessEqual,
    GreaterThan,
    GreaterEqual,
    Concat
};

enum class DelimiterType {
    LeftParen,
    RightParen,
    Comma,
    Semicolon,
    Dot,
    Star
};

enum class KeywordType {
    // DML (Data Manipulation Language)
    SELECT,
    FROM,
    WHERE,
    
    // Filtering & Conditions
    BETWEEN,
    DISTINCT,
    LIKE,
    IN,
    
    // Grouping & Ordering
    GROUP,
    BY,
    ORDER,
    HAVING,
    ASC,
    DESC,
    
    // Joins
    JOIN,
    INNER,
    LEFT,
    RIGHT,
    FULL,
    OUTER,
    CROSS,
    ON,
    USING,
    
    // Logical Operators
    AND,
    OR,
    NOT,
    
    // Limit & Pagination
    LIMIT,
    OFFSET,
    TOP,
    FETCH,
    
    // Aggregate Functions
    MIN,
    MAX,
    COUNT,
    SUM,
    AVG,
    
    // Data Types (for CAST operations only - users cannot create tables)
    // Integer Types
    INT,
    INTEGER,
    BIGINT,

    // Floating Point Types
    FLOAT,
    DOUBLE,
    DECIMAL,

    // String Type
    TEXT,

    // Boolean Type
    BOOLEAN,

    // Date/Time Types
    DATE,
    TIME,
    TIMESTAMP,
    
    // Set Operations
    UNION,
    INTERSECT,
    EXCEPT,
    MINUS,
    
    // Case Expressions
    CASE,
    WHEN,
    THEN,
    ELSE,
    END,
    
    // Other Common Keywords
    AS,
    ALL,
    ANY,
    SOME,
    EXISTS,
    NULL_KW,
    CAST,
    IS,
    TRUE_KW,
    FALSE_KW,
};

struct Token {
    std::string text;
    TokenType type;
    size_t line{1};
    size_t column{1};

    std::optional<KeywordType> keywordType;
    std::optional<OperatorType> operatorType;
    std::optional<DelimiterType> delimiterType;
    std::optional<SqlLiteral> literalValue;
};