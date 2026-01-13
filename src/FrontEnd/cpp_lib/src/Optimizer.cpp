#include "Optimizer.hpp"
#include <cmath>

namespace optisql {

SelectStatement Optimizer::optimize(SelectStatement stmt) {
    for (auto& item : stmt.select.items) {
        if (item.expression) {
            item.expression = optimizeExpression(std::move(item.expression));
        }
    }

    if (stmt.where) {
        stmt.where->condition = optimizeExpression(std::move(stmt.where->condition));
    }

    if (stmt.having) {
        stmt.having->condition = optimizeExpression(std::move(stmt.having->condition));
    }

    if (stmt.groupBy) {
        for (auto& expr : stmt.groupBy->expressions) {
            expr = optimizeExpression(std::move(expr));
        }
    }

    if (stmt.orderBy) {
        for (auto& item : stmt.orderBy->items) {
            item.expression = optimizeExpression(std::move(item.expression));
        }
    }

    if (stmt.limit) {
        stmt.limit->count = optimizeExpression(std::move(stmt.limit->count));
        if (stmt.limit->offset) {
            stmt.limit->offset = optimizeExpression(std::move(*stmt.limit->offset));
        }
    }

    if (stmt.from) {
        for (auto& join : stmt.from->joins) {
            if (join.condition) {
                join.condition = optimizeExpression(std::move(*join.condition));
            }
        }
    }

    return stmt;
}

ExprPtr Optimizer::optimizeExpression(ExprPtr expr) {
    if (!expr) return expr;

    expr = foldConstants(std::move(expr));
    expr = simplifyNumerical(std::move(expr));
    expr = simplifyPredicates(std::move(expr));

    return expr;
}

ExprPtr Optimizer::foldConstants(ExprPtr expr) {
    if (!expr) return expr;

    if (auto* binary = std::get_if<BinaryExpr>(expr.get())) {
        binary->left = foldConstants(std::move(binary->left));
        binary->right = foldConstants(std::move(binary->right));

        auto leftVal = tryEvaluate(*binary->left);
        auto rightVal = tryEvaluate(*binary->right);

        if (leftVal && rightVal) {
            auto leftNum = toDouble(*leftVal);
            auto rightNum = toDouble(*rightVal);

            if (leftNum && rightNum) {
                double result = 0;
                bool valid = true;

                switch (binary->op) {
                    case OperatorType::Plus:
                        result = *leftNum + *rightNum;
                        break;
                    case OperatorType::Minus:
                        result = *leftNum - *rightNum;
                        break;
                    case OperatorType::Multiply:
                        result = *leftNum * *rightNum;
                        break;
                    case OperatorType::Divide:
                        if (*rightNum != 0) {
                            result = *leftNum / *rightNum;
                        } else {
                            valid = false;
                        }
                        break;
                    case OperatorType::Modulo: {
                        auto leftLong = toLong(*leftVal);
                        auto rightLong = toLong(*rightVal);
                        if (leftLong && rightLong && *rightLong != 0) {
                            result = static_cast<double>(*leftLong % *rightLong);
                        } else {
                            valid = false;
                        }
                        break;
                    }
                    default:
                        valid = false;
                }

                if (valid) {
                    LiteralExpr lit;
                    if (std::floor(result) == result && result >= LONG_MIN && result <= LONG_MAX) {
                        lit.value = static_cast<long>(result);
                    } else {
                        lit.value = result;
                    }
                    lit.line = binary->line;
                    lit.column = binary->column;
                    return makeExpr(std::move(lit));
                }
            }

            auto leftBool = toBool(*leftVal);
            auto rightBool = toBool(*rightVal);

            if (leftBool && rightBool) {
                bool result = false;
                bool valid = true;

                switch (binary->op) {
                    default:
                        valid = false;
                }

                if (valid) {
                    LiteralExpr lit;
                    lit.value = result;
                    lit.line = binary->line;
                    lit.column = binary->column;
                    return makeExpr(std::move(lit));
                }
            }

            if (leftNum && rightNum) {
                bool result = false;
                bool valid = true;

                switch (binary->op) {
                    case OperatorType::Equal:
                        result = *leftNum == *rightNum;
                        break;
                    case OperatorType::NotEqual:
                        result = *leftNum != *rightNum;
                        break;
                    case OperatorType::LessThan:
                        result = *leftNum < *rightNum;
                        break;
                    case OperatorType::LessEqual:
                        result = *leftNum <= *rightNum;
                        break;
                    case OperatorType::GreaterThan:
                        result = *leftNum > *rightNum;
                        break;
                    case OperatorType::GreaterEqual:
                        result = *leftNum >= *rightNum;
                        break;
                    default:
                        valid = false;
                }

                if (valid) {
                    LiteralExpr lit;
                    lit.value = result;
                    lit.line = binary->line;
                    lit.column = binary->column;
                    return makeExpr(std::move(lit));
                }
            }

            if (std::holds_alternative<std::string>(*leftVal) &&
                std::holds_alternative<std::string>(*rightVal)) {
                const auto& leftStr = std::get<std::string>(*leftVal);
                const auto& rightStr = std::get<std::string>(*rightVal);

                if (binary->op == OperatorType::Concat) {
                    LiteralExpr lit;
                    lit.value = leftStr + rightStr;
                    lit.line = binary->line;
                    lit.column = binary->column;
                    return makeExpr(std::move(lit));
                }
            }
        }
    }

    if (auto* unary = std::get_if<UnaryExpr>(expr.get())) {
        unary->operand = foldConstants(std::move(unary->operand));

        auto val = tryEvaluate(*unary->operand);
        if (val) {
            if (unary->op == UnaryExpr::Op::Minus) {
                auto num = toDouble(*val);
                if (num) {
                    LiteralExpr lit;
                    if (auto longVal = toLong(*val)) {
                        lit.value = -(*longVal);
                    } else {
                        lit.value = -(*num);
                    }
                    lit.line = unary->line;
                    lit.column = unary->column;
                    return makeExpr(std::move(lit));
                }
            } else if (unary->op == UnaryExpr::Op::Not) {
                auto b = toBool(*val);
                if (b) {
                    LiteralExpr lit;
                    lit.value = !(*b);
                    lit.line = unary->line;
                    lit.column = unary->column;
                    return makeExpr(std::move(lit));
                }
            }
        }
    }

    if (auto* func = std::get_if<FunctionCallExpr>(expr.get())) {
        for (auto& arg : func->arguments) {
            arg = foldConstants(std::move(arg));
        }
    }

    if (auto* caseExpr = std::get_if<CaseExpr>(expr.get())) {
        if (caseExpr->operand) {
            caseExpr->operand = foldConstants(std::move(*caseExpr->operand));
        }
        for (auto& whenClause : caseExpr->whenClauses) {
            whenClause.condition = foldConstants(std::move(whenClause.condition));
            whenClause.result = foldConstants(std::move(whenClause.result));
        }
        if (caseExpr->elseClause) {
            caseExpr->elseClause = foldConstants(std::move(*caseExpr->elseClause));
        }
    }

    if (auto* between = std::get_if<BetweenExpr>(expr.get())) {
        between->value = foldConstants(std::move(between->value));
        between->low = foldConstants(std::move(between->low));
        between->high = foldConstants(std::move(between->high));
    }

    if (auto* inExpr = std::get_if<InExpr>(expr.get())) {
        inExpr->value = foldConstants(std::move(inExpr->value));
        if (auto* values = std::get_if<std::vector<ExprPtr>>(&inExpr->list)) {
            for (auto& val : *values) {
                val = foldConstants(std::move(val));
            }
        }
    }

    if (auto* cast = std::get_if<CastExpr>(expr.get())) {
        cast->value = foldConstants(std::move(cast->value));
    }

    return expr;
}

ExprPtr Optimizer::simplifyNumerical(ExprPtr expr) {
    if (!expr) return expr;

    if (auto* binary = std::get_if<BinaryExpr>(expr.get())) {
        binary->left = simplifyNumerical(std::move(binary->left));
        binary->right = simplifyNumerical(std::move(binary->right));

        switch (binary->op) {
            case OperatorType::Plus:
                if (isZero(*binary->left)) return std::move(binary->right);
                if (isZero(*binary->right)) return std::move(binary->left);
                break;
            case OperatorType::Minus:
                if (isZero(*binary->right)) return std::move(binary->left);
                break;
            case OperatorType::Multiply:
                if (isZero(*binary->left) || isZero(*binary->right)) {
                    LiteralExpr lit;
                    lit.value = 0L;
                    lit.line = binary->line;
                    lit.column = binary->column;
                    return makeExpr(std::move(lit));
                }
                if (isOne(*binary->left)) return std::move(binary->right);
                if (isOne(*binary->right)) return std::move(binary->left);
                break;
            case OperatorType::Divide:
                if (isOne(*binary->right)) return std::move(binary->left);
                break;
            default:
                break;
        }
    }

    if (auto* unary = std::get_if<UnaryExpr>(expr.get())) {
        unary->operand = simplifyNumerical(std::move(unary->operand));
    }

    if (auto* func = std::get_if<FunctionCallExpr>(expr.get())) {
        for (auto& arg : func->arguments) {
            arg = simplifyNumerical(std::move(arg));
        }
    }

    return expr;
}

ExprPtr Optimizer::simplifyPredicates(ExprPtr expr) {
    if (!expr) return expr;

    if (auto* binary = std::get_if<BinaryExpr>(expr.get())) {
        binary->left = simplifyPredicates(std::move(binary->left));
        binary->right = simplifyPredicates(std::move(binary->right));
    }

    if (auto* unary = std::get_if<UnaryExpr>(expr.get())) {
        unary->operand = simplifyPredicates(std::move(unary->operand));

        if (unary->op == UnaryExpr::Op::Not) {
            if (isTrue(*unary->operand)) {
                LiteralExpr lit;
                lit.value = false;
                lit.line = unary->line;
                lit.column = unary->column;
                return makeExpr(std::move(lit));
            }
            if (isFalse(*unary->operand)) {
                LiteralExpr lit;
                lit.value = true;
                lit.line = unary->line;
                lit.column = unary->column;
                return makeExpr(std::move(lit));
            }
            if (auto* innerUnary = std::get_if<UnaryExpr>(unary->operand.get())) {
                if (innerUnary->op == UnaryExpr::Op::Not) {
                    return std::move(innerUnary->operand);
                }
            }
        }
    }

    if (auto* isNullExpr = std::get_if<IsNullExpr>(expr.get())) {
        isNullExpr->value = simplifyPredicates(std::move(isNullExpr->value));

        if (isNull(*isNullExpr->value)) {
            LiteralExpr lit;
            lit.value = !isNullExpr->negated;
            lit.line = isNullExpr->line;
            lit.column = isNullExpr->column;
            return makeExpr(std::move(lit));
        }

        auto val = tryEvaluate(*isNullExpr->value);
        if (val && !std::holds_alternative<Null>(*val)) {
            LiteralExpr lit;
            lit.value = isNullExpr->negated;
            lit.line = isNullExpr->line;
            lit.column = isNullExpr->column;
            return makeExpr(std::move(lit));
        }
    }

    return expr;
}

bool Optimizer::isZero(const Expression& expr) {
    if (auto* lit = std::get_if<LiteralExpr>(&expr)) {
        if (auto* i = std::get_if<long>(&lit->value)) {
            return *i == 0;
        }
        if (auto* d = std::get_if<double>(&lit->value)) {
            return *d == 0.0;
        }
    }
    return false;
}

bool Optimizer::isOne(const Expression& expr) {
    if (auto* lit = std::get_if<LiteralExpr>(&expr)) {
        if (auto* i = std::get_if<long>(&lit->value)) {
            return *i == 1;
        }
        if (auto* d = std::get_if<double>(&lit->value)) {
            return *d == 1.0;
        }
    }
    return false;
}

bool Optimizer::isTrue(const Expression& expr) {
    if (auto* lit = std::get_if<LiteralExpr>(&expr)) {
        if (auto* b = std::get_if<bool>(&lit->value)) {
            return *b == true;
        }
    }
    return false;
}

bool Optimizer::isFalse(const Expression& expr) {
    if (auto* lit = std::get_if<LiteralExpr>(&expr)) {
        if (auto* b = std::get_if<bool>(&lit->value)) {
            return *b == false;
        }
    }
    return false;
}

bool Optimizer::isNull(const Expression& expr) {
    if (auto* lit = std::get_if<LiteralExpr>(&expr)) {
        return std::holds_alternative<Null>(lit->value);
    }
    return false;
}

std::optional<SqlLiteral> Optimizer::tryEvaluate(const Expression& expr) {
    if (auto* lit = std::get_if<LiteralExpr>(&expr)) {
        return lit->value;
    }
    return std::nullopt;
}

std::optional<double> Optimizer::toDouble(const SqlLiteral& val) {
    if (auto* i = std::get_if<long>(&val)) {
        return static_cast<double>(*i);
    }
    if (auto* d = std::get_if<double>(&val)) {
        return *d;
    }
    return std::nullopt;
}

std::optional<long> Optimizer::toLong(const SqlLiteral& val) {
    if (auto* i = std::get_if<long>(&val)) {
        return *i;
    }
    return std::nullopt;
}

std::optional<bool> Optimizer::toBool(const SqlLiteral& val) {
    if (auto* b = std::get_if<bool>(&val)) {
        return *b;
    }
    return std::nullopt;
}

} // namespace optisql
