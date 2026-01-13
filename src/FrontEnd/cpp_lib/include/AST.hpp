#pragma once
#include "Token.hpp"
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

struct SelectStatement;
using StmtPtr = std::unique_ptr<SelectStatement>;

struct LiteralExpr {
    SqlLiteral value;
    size_t line{0};
    size_t column{0};
};

struct IdentifierExpr {
    std::vector<std::string> parts;
    size_t line{0};
    size_t column{0};
};

struct StarExpr {
    std::optional<std::string> table;
    size_t line{0};
    size_t column{0};
};

struct Expression;
using ExprPtr = std::unique_ptr<Expression>;

struct BinaryExpr {
    ExprPtr left;
    OperatorType op;
    ExprPtr right;
    size_t line{0};
    size_t column{0};
};

struct UnaryExpr {
    enum class Op { Minus, Not };
    Op op;
    ExprPtr operand;
    size_t line{0};
    size_t column{0};
};

struct FunctionCallExpr {
    std::string name;
    std::vector<ExprPtr> arguments;
    bool distinct{false};
    size_t line{0};
    size_t column{0};
};

struct CaseExpr {
    std::optional<ExprPtr> operand;
    struct WhenClause {
        ExprPtr condition;
        ExprPtr result;
    };
    std::vector<WhenClause> whenClauses;
    std::optional<ExprPtr> elseClause;
    size_t line{0};
    size_t column{0};
};

struct BetweenExpr {
    ExprPtr value;
    ExprPtr low;
    ExprPtr high;
    bool negated{false};
    size_t line{0};
    size_t column{0};
};

struct InExpr {
    ExprPtr value;
    std::variant<std::vector<ExprPtr>, StmtPtr> list;
    bool negated{false};
    size_t line{0};
    size_t column{0};
};

struct LikeExpr {
    ExprPtr value;
    ExprPtr pattern;
    bool negated{false};
    size_t line{0};
    size_t column{0};
};

struct IsNullExpr {
    ExprPtr value;
    bool negated{false};
    size_t line{0};
    size_t column{0};
};

struct CastExpr {
    ExprPtr value;
    KeywordType targetType;
    size_t line{0};
    size_t column{0};
};

struct SubqueryExpr {
    StmtPtr query;
    size_t line{0};
    size_t column{0};
};

struct ExistsExpr {
    StmtPtr query;
    size_t line{0};
    size_t column{0};
};

using ExpressionVariant = std::variant<
    LiteralExpr,
    IdentifierExpr,
    BinaryExpr,
    UnaryExpr,
    FunctionCallExpr,
    CaseExpr,
    BetweenExpr,
    InExpr,
    LikeExpr,
    IsNullExpr,
    CastExpr,
    SubqueryExpr,
    ExistsExpr,
    StarExpr
>;

struct Expression : ExpressionVariant {
    using ExpressionVariant::ExpressionVariant;
    using ExpressionVariant::operator=;
};

struct SelectItem {
    ExprPtr expression;
    std::optional<std::string> alias;
};

struct SelectClause {
    bool distinct{false};
    std::vector<SelectItem> items;
};

struct TableRef {
    struct BaseTable {
        std::string name;
        std::optional<std::string> schema;
        std::optional<std::string> alias;
    };
    struct SubqueryTable {
        StmtPtr query;
        std::string alias;
    };
    std::variant<BaseTable, SubqueryTable> source;
};

enum class JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross
};

struct JoinClause {
    JoinType type;
    TableRef table;
    std::optional<ExprPtr> condition;
};

struct FromClause {
    TableRef primary;
    std::vector<JoinClause> joins;
};

struct WhereClause {
    ExprPtr condition;
};

struct GroupByClause {
    std::vector<ExprPtr> expressions;
};

struct HavingClause {
    ExprPtr condition;
};

struct OrderByItem {
    ExprPtr expression;
    bool ascending{true};
};

struct OrderByClause {
    std::vector<OrderByItem> items;
};

struct LimitClause {
    ExprPtr count;
    std::optional<ExprPtr> offset;
};

enum class SetOperation {
    None,
    Union,
    UnionAll,
    Intersect,
    Except
};

struct SelectStatement {
    SelectClause select;
    std::optional<FromClause> from;
    std::optional<WhereClause> where;
    std::optional<GroupByClause> groupBy;
    std::optional<HavingClause> having;
    std::optional<OrderByClause> orderBy;
    std::optional<LimitClause> limit;
    SetOperation setOp{SetOperation::None};
    StmtPtr rightQuery;
};

template<typename T>
inline ExprPtr makeExpr(T&& expr) {
    return std::make_unique<Expression>(std::forward<T>(expr));
}

inline StmtPtr makeStmt() {
    return std::make_unique<SelectStatement>();
}
