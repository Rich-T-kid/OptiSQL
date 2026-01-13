#pragma once

#include "AST.hpp"
#include <nlohmann/json.hpp>

namespace optisql {

using json = nlohmann::json;

class JSONSerializer {
public:
    json serialize(const SelectStatement& stmt);
    json serializeExpression(const Expression& expr);

private:
    json serializeLiteral(const LiteralExpr& lit);
    json serializeIdentifier(const IdentifierExpr& ident);
    json serializeStar(const StarExpr& star);
    json serializeBinary(const BinaryExpr& binary);
    json serializeUnary(const UnaryExpr& unary);
    json serializeFunction(const FunctionCallExpr& func);
    json serializeCase(const CaseExpr& caseExpr);
    json serializeBetween(const BetweenExpr& between);
    json serializeIn(const InExpr& in);
    json serializeLike(const LikeExpr& like);
    json serializeIsNull(const IsNullExpr& isNull);
    json serializeCast(const CastExpr& cast);
    json serializeSubquery(const SubqueryExpr& subquery);
    json serializeExists(const ExistsExpr& exists);

    json serializeSelectClause(const SelectClause& select);
    json serializeFromClause(const FromClause& from);
    json serializeTableRef(const TableRef& table);
    json serializeJoinClause(const JoinClause& join);

    std::string operatorToString(OperatorType op);
    std::string keywordToString(KeywordType kw);
    std::string joinTypeToString(JoinType jt);
    std::string setOpToString(SetOperation op);
};

} // namespace optisql
