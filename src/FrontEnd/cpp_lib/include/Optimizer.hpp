#pragma once

#include "AST.hpp"
#include <unordered_set>
#include <string>

namespace optisql {

class Optimizer {
public:
    SelectStatement optimize(SelectStatement stmt);
    ExprPtr optimizeExpression(ExprPtr expr);

private:
    ExprPtr foldConstants(ExprPtr expr);
    ExprPtr simplifyNumerical(ExprPtr expr);
    ExprPtr simplifyPredicates(ExprPtr expr);

    bool isZero(const Expression& expr);
    bool isOne(const Expression& expr);
    bool isTrue(const Expression& expr);
    bool isFalse(const Expression& expr);
    bool isNull(const Expression& expr);

    std::optional<SqlLiteral> tryEvaluate(const Expression& expr);
    std::optional<double> toDouble(const SqlLiteral& val);
    std::optional<long> toLong(const SqlLiteral& val);
    std::optional<bool> toBool(const SqlLiteral& val);
};

} // namespace optisql
