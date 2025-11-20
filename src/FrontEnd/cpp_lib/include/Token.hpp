# pragma once
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
    
    // Integer Types
    INT,
    INTEGER,
    TINYINT,
    SMALLINT,
    MEDIUMINT,
    BIGINT,
    INT2,
    INT4,
    INT8,
    
    // Floating Point Types
    FLOAT,
    FLOAT4,
    FLOAT8,
    FLOAT32,
    FLOAT64,
    DOUBLE,
    REAL,
    DECIMAL,
    NUMERIC,
    
    // String Types
    VARCHAR,
    TEXT,
    
    // Boolean Types
    BOOLEAN,
    BOOL,
    
    // Date/Time Types
    DATE,
    TIME,
    DATETIME,
    TIMESTAMP,
    YEAR,
    INTERVAL,
    
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

};

struct Token {
    // Simple value for most tokens
    std::string text;  // The raw text from source
    TokenType type;

    
    // Additional parsed data depending on type
    std::optional<std::vector<std::string>> qualifiedName;  // For identifiers
    std::optional<SqlLiteral> literalValue;                 // For literals
};