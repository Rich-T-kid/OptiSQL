#include "JSONSerializer.hpp"

namespace optisql {

json JSONSerializer::serialize(const SelectStatement& stmt) {
    json result;
    result["type"] = "SelectStatement";

    result["select"] = serializeSelectClause(stmt.select);

    if (stmt.from) {
        result["from"] = serializeFromClause(*stmt.from);
    }

    if (stmt.where) {
        result["where"] = serializeExpression(*stmt.where->condition);
    }

    if (stmt.groupBy) {
        json groupBy = json::array();
        for (const auto& expr : stmt.groupBy->expressions) {
            groupBy.push_back(serializeExpression(*expr));
        }
        result["groupBy"] = groupBy;
    }

    if (stmt.having) {
        result["having"] = serializeExpression(*stmt.having->condition);
    }

    if (stmt.orderBy) {
        json orderBy = json::array();
        for (const auto& item : stmt.orderBy->items) {
            json orderItem;
            orderItem["expression"] = serializeExpression(*item.expression);
            orderItem["ascending"] = item.ascending;
            orderBy.push_back(orderItem);
        }
        result["orderBy"] = orderBy;
    }

    if (stmt.limit) {
        json limit;
        limit["count"] = serializeExpression(*stmt.limit->count);
        if (stmt.limit->offset) {
            limit["offset"] = serializeExpression(**stmt.limit->offset);
        }
        result["limit"] = limit;
    }

    if (stmt.setOp != SetOperation::None) {
        result["setOperation"] = setOpToString(stmt.setOp);
        if (stmt.rightQuery) {
            result["rightQuery"] = serialize(*stmt.rightQuery);
        }
    }

    return result;
}

json JSONSerializer::serializeExpression(const Expression& expr) {
    return std::visit([this](const auto& e) -> json {
        using T = std::decay_t<decltype(e)>;
        if constexpr (std::is_same_v<T, LiteralExpr>) {
            return serializeLiteral(e);
        } else if constexpr (std::is_same_v<T, IdentifierExpr>) {
            return serializeIdentifier(e);
        } else if constexpr (std::is_same_v<T, StarExpr>) {
            return serializeStar(e);
        } else if constexpr (std::is_same_v<T, BinaryExpr>) {
            return serializeBinary(e);
        } else if constexpr (std::is_same_v<T, UnaryExpr>) {
            return serializeUnary(e);
        } else if constexpr (std::is_same_v<T, FunctionCallExpr>) {
            return serializeFunction(e);
        } else if constexpr (std::is_same_v<T, CaseExpr>) {
            return serializeCase(e);
        } else if constexpr (std::is_same_v<T, BetweenExpr>) {
            return serializeBetween(e);
        } else if constexpr (std::is_same_v<T, InExpr>) {
            return serializeIn(e);
        } else if constexpr (std::is_same_v<T, LikeExpr>) {
            return serializeLike(e);
        } else if constexpr (std::is_same_v<T, IsNullExpr>) {
            return serializeIsNull(e);
        } else if constexpr (std::is_same_v<T, CastExpr>) {
            return serializeCast(e);
        } else if constexpr (std::is_same_v<T, SubqueryExpr>) {
            return serializeSubquery(e);
        } else if constexpr (std::is_same_v<T, ExistsExpr>) {
            return serializeExists(e);
        }
    }, static_cast<const ExpressionVariant&>(expr));
}

json JSONSerializer::serializeLiteral(const LiteralExpr& lit) {
    json result;
    result["type"] = "Literal";

    std::visit([&result](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::string>) {
            result["dataType"] = "string";
            result["value"] = v;
        } else if constexpr (std::is_same_v<T, long>) {
            result["dataType"] = "integer";
            result["value"] = v;
        } else if constexpr (std::is_same_v<T, double>) {
            result["dataType"] = "float";
            result["value"] = v;
        } else if constexpr (std::is_same_v<T, bool>) {
            result["dataType"] = "boolean";
            result["value"] = v;
        } else if constexpr (std::is_same_v<T, Null>) {
            result["dataType"] = "null";
            result["value"] = nullptr;
        }
    }, lit.value);

    return result;
}

json JSONSerializer::serializeIdentifier(const IdentifierExpr& ident) {
    json result;
    result["type"] = "Identifier";
    result["parts"] = ident.parts;
    return result;
}

json JSONSerializer::serializeStar(const StarExpr& star) {
    json result;
    result["type"] = "Star";
    if (star.table) {
        result["table"] = *star.table;
    }
    return result;
}

json JSONSerializer::serializeBinary(const BinaryExpr& binary) {
    json result;
    result["type"] = "BinaryExpr";
    result["operator"] = operatorToString(binary.op);
    result["left"] = serializeExpression(*binary.left);
    result["right"] = serializeExpression(*binary.right);
    return result;
}

json JSONSerializer::serializeUnary(const UnaryExpr& unary) {
    json result;
    result["type"] = "UnaryExpr";
    result["operator"] = (unary.op == UnaryExpr::Op::Minus) ? "-" : "NOT";
    result["operand"] = serializeExpression(*unary.operand);
    return result;
}

json JSONSerializer::serializeFunction(const FunctionCallExpr& func) {
    json result;
    result["type"] = "FunctionCall";
    result["name"] = func.name;
    result["distinct"] = func.distinct;

    json args = json::array();
    for (const auto& arg : func.arguments) {
        args.push_back(serializeExpression(*arg));
    }
    result["arguments"] = args;

    return result;
}

json JSONSerializer::serializeCase(const CaseExpr& caseExpr) {
    json result;
    result["type"] = "CaseExpr";

    if (caseExpr.operand) {
        result["operand"] = serializeExpression(**caseExpr.operand);
    }

    json whenClauses = json::array();
    for (const auto& wc : caseExpr.whenClauses) {
        json whenClause;
        whenClause["condition"] = serializeExpression(*wc.condition);
        whenClause["result"] = serializeExpression(*wc.result);
        whenClauses.push_back(whenClause);
    }
    result["whenClauses"] = whenClauses;

    if (caseExpr.elseClause) {
        result["else"] = serializeExpression(**caseExpr.elseClause);
    }

    return result;
}

json JSONSerializer::serializeBetween(const BetweenExpr& between) {
    json result;
    result["type"] = "BetweenExpr";
    result["value"] = serializeExpression(*between.value);
    result["low"] = serializeExpression(*between.low);
    result["high"] = serializeExpression(*between.high);
    result["negated"] = between.negated;
    return result;
}

json JSONSerializer::serializeIn(const InExpr& in) {
    json result;
    result["type"] = "InExpr";
    result["value"] = serializeExpression(*in.value);
    result["negated"] = in.negated;

    if (auto* values = std::get_if<std::vector<ExprPtr>>(&in.list)) {
        json list = json::array();
        for (const auto& v : *values) {
            list.push_back(serializeExpression(*v));
        }
        result["list"] = list;
    } else if (auto* subquery = std::get_if<StmtPtr>(&in.list)) {
        result["subquery"] = serialize(**subquery);
    }

    return result;
}

json JSONSerializer::serializeLike(const LikeExpr& like) {
    json result;
    result["type"] = "LikeExpr";
    result["value"] = serializeExpression(*like.value);
    result["pattern"] = serializeExpression(*like.pattern);
    result["negated"] = like.negated;
    return result;
}

json JSONSerializer::serializeIsNull(const IsNullExpr& isNull) {
    json result;
    result["type"] = "IsNullExpr";
    result["value"] = serializeExpression(*isNull.value);
    result["negated"] = isNull.negated;
    return result;
}

json JSONSerializer::serializeCast(const CastExpr& cast) {
    json result;
    result["type"] = "CastExpr";
    result["value"] = serializeExpression(*cast.value);
    result["targetType"] = keywordToString(cast.targetType);
    return result;
}

json JSONSerializer::serializeSubquery(const SubqueryExpr& subquery) {
    json result;
    result["type"] = "Subquery";
    result["query"] = serialize(*subquery.query);
    return result;
}

json JSONSerializer::serializeExists(const ExistsExpr& exists) {
    json result;
    result["type"] = "ExistsExpr";
    result["query"] = serialize(*exists.query);
    return result;
}

json JSONSerializer::serializeSelectClause(const SelectClause& select) {
    json result;
    result["distinct"] = select.distinct;

    json items = json::array();
    for (const auto& item : select.items) {
        json selectItem;
        if (item.expression) {
            selectItem["expression"] = serializeExpression(*item.expression);
        }
        if (item.alias) {
            selectItem["alias"] = *item.alias;
        }
        items.push_back(selectItem);
    }
    result["items"] = items;

    return result;
}

json JSONSerializer::serializeFromClause(const FromClause& from) {
    json result;
    result["primary"] = serializeTableRef(from.primary);

    if (!from.joins.empty()) {
        json joins = json::array();
        for (const auto& join : from.joins) {
            joins.push_back(serializeJoinClause(join));
        }
        result["joins"] = joins;
    }

    return result;
}

json JSONSerializer::serializeTableRef(const TableRef& table) {
    json result;

    std::visit([&result, this](const auto& src) {
        using T = std::decay_t<decltype(src)>;
        if constexpr (std::is_same_v<T, TableRef::BaseTable>) {
            result["type"] = "BaseTable";
            result["name"] = src.name;
            if (src.schema) {
                result["schema"] = *src.schema;
            }
            if (src.alias) {
                result["alias"] = *src.alias;
            }
        } else if constexpr (std::is_same_v<T, TableRef::SubqueryTable>) {
            result["type"] = "SubqueryTable";
            result["query"] = serialize(*src.query);
            result["alias"] = src.alias;
        }
    }, table.source);

    return result;
}

json JSONSerializer::serializeJoinClause(const JoinClause& join) {
    json result;
    result["type"] = joinTypeToString(join.type);
    result["table"] = serializeTableRef(join.table);
    if (join.condition) {
        result["condition"] = serializeExpression(**join.condition);
    }
    return result;
}

std::string JSONSerializer::operatorToString(OperatorType op) {
    switch (op) {
        case OperatorType::Plus: return "+";
        case OperatorType::Minus: return "-";
        case OperatorType::Multiply: return "*";
        case OperatorType::Divide: return "/";
        case OperatorType::Modulo: return "%";
        case OperatorType::Equal: return "=";
        case OperatorType::NotEqual: return "!=";
        case OperatorType::LessThan: return "<";
        case OperatorType::LessEqual: return "<=";
        case OperatorType::GreaterThan: return ">";
        case OperatorType::GreaterEqual: return ">=";
        case OperatorType::Concat: return "||";
    }
    return "UNKNOWN";
}

std::string JSONSerializer::keywordToString(KeywordType kw) {
    switch (kw) {
        case KeywordType::INT: return "INT";
        case KeywordType::INTEGER: return "INTEGER";
        case KeywordType::BIGINT: return "BIGINT";
        case KeywordType::FLOAT: return "FLOAT";
        case KeywordType::DOUBLE: return "DOUBLE";
        case KeywordType::DECIMAL: return "DECIMAL";
        case KeywordType::TEXT: return "TEXT";
        case KeywordType::BOOLEAN: return "BOOLEAN";
        case KeywordType::DATE: return "DATE";
        case KeywordType::TIME: return "TIME";
        case KeywordType::TIMESTAMP: return "TIMESTAMP";
        default: return "UNKNOWN";
    }
}

std::string JSONSerializer::joinTypeToString(JoinType jt) {
    switch (jt) {
        case JoinType::Inner: return "INNER";
        case JoinType::Left: return "LEFT";
        case JoinType::Right: return "RIGHT";
        case JoinType::Full: return "FULL";
        case JoinType::Cross: return "CROSS";
    }
    return "UNKNOWN";
}

std::string JSONSerializer::setOpToString(SetOperation op) {
    switch (op) {
        case SetOperation::None: return "NONE";
        case SetOperation::Union: return "UNION";
        case SetOperation::UnionAll: return "UNION ALL";
        case SetOperation::Intersect: return "INTERSECT";
        case SetOperation::Except: return "EXCEPT";
    }
    return "UNKNOWN";
}

} // namespace optisql
