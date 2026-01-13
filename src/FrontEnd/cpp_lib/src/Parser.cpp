#include "Parser.hpp"
#include <algorithm>

Parser::Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

std::expected<StmtPtr, ParseError> Parser::parse() {
    return parseSelectStatement();
}

std::expected<StmtPtr, ParseError> Parser::parseSelectStatement() {
    auto stmt = makeStmt();

    auto selectResult = parseSelectClause();
    if (!selectResult) return std::unexpected(selectResult.error());
    stmt->select = std::move(*selectResult);

    if (checkKeyword(KeywordType::FROM)) {
        auto fromResult = parseFromClause();
        if (!fromResult) return std::unexpected(fromResult.error());
        stmt->from = std::move(*fromResult);
    }

    if (checkKeyword(KeywordType::WHERE)) {
        auto whereResult = parseWhereClause();
        if (!whereResult) return std::unexpected(whereResult.error());
        stmt->where = std::move(*whereResult);
    }

    if (checkKeyword(KeywordType::GROUP)) {
        auto groupResult = parseGroupByClause();
        if (!groupResult) return std::unexpected(groupResult.error());
        stmt->groupBy = std::move(*groupResult);
    }

    if (checkKeyword(KeywordType::HAVING)) {
        auto havingResult = parseHavingClause();
        if (!havingResult) return std::unexpected(havingResult.error());
        stmt->having = std::move(*havingResult);
    }

    if (checkKeyword(KeywordType::ORDER)) {
        auto orderResult = parseOrderByClause();
        if (!orderResult) return std::unexpected(orderResult.error());
        stmt->orderBy = std::move(*orderResult);
    }

    if (checkKeyword(KeywordType::LIMIT)) {
        auto limitResult = parseLimitClause();
        if (!limitResult) return std::unexpected(limitResult.error());
        stmt->limit = std::move(*limitResult);
    }

    if (checkKeyword(KeywordType::UNION)) {
        advance();
        if (matchKeyword(KeywordType::ALL)) {
            stmt->setOp = SetOperation::UnionAll;
        } else {
            stmt->setOp = SetOperation::Union;
        }
        auto rightResult = parseSelectStatement();
        if (!rightResult) return std::unexpected(rightResult.error());
        stmt->rightQuery = std::move(*rightResult);
    } else if (checkKeyword(KeywordType::INTERSECT)) {
        advance();
        stmt->setOp = SetOperation::Intersect;
        auto rightResult = parseSelectStatement();
        if (!rightResult) return std::unexpected(rightResult.error());
        stmt->rightQuery = std::move(*rightResult);
    } else if (checkKeyword(KeywordType::EXCEPT)) {
        advance();
        stmt->setOp = SetOperation::Except;
        auto rightResult = parseSelectStatement();
        if (!rightResult) return std::unexpected(rightResult.error());
        stmt->rightQuery = std::move(*rightResult);
    }

    return stmt;
}

std::expected<SelectClause, ParseError> Parser::parseSelectClause() {
    auto selectToken = consumeKeyword(KeywordType::SELECT, "Expected SELECT");
    if (!selectToken) return std::unexpected(selectToken.error());

    SelectClause clause;

    if (matchKeyword(KeywordType::DISTINCT)) {
        clause.distinct = true;
    } else if (matchKeyword(KeywordType::ALL)) {
        clause.distinct = false;
    }

    do {
        auto item = parseSelectItem();
        if (!item) return std::unexpected(item.error());
        clause.items.push_back(std::move(*item));
    } while (matchDelimiter(DelimiterType::Comma));

    return clause;
}

std::expected<SelectItem, ParseError> Parser::parseSelectItem() {
    SelectItem item;

    if (checkDelimiter(DelimiterType::Star)) {
        advance();
        StarExpr star;
        star.line = previous().line;
        star.column = previous().column;
        item.expression = makeExpr(std::move(star));
    } else {
        auto expr = parseExpression();
        if (!expr) return std::unexpected(expr.error());
        item.expression = std::move(*expr);
    }

    if (matchKeyword(KeywordType::AS)) {
        if (!check(TokenType::Identifier)) {
            return std::unexpected(error(peek(), "Expected alias after AS"));
        }
        item.alias = advance().text;
    } else if (check(TokenType::Identifier) && !checkKeyword(KeywordType::FROM) &&
               !checkKeyword(KeywordType::WHERE) && !checkKeyword(KeywordType::GROUP) &&
               !checkKeyword(KeywordType::ORDER) && !checkKeyword(KeywordType::LIMIT) &&
               !checkKeyword(KeywordType::HAVING) && !checkKeyword(KeywordType::UNION) &&
               !checkKeyword(KeywordType::INTERSECT) && !checkKeyword(KeywordType::EXCEPT) &&
               !checkKeyword(KeywordType::JOIN) && !checkKeyword(KeywordType::INNER) &&
               !checkKeyword(KeywordType::LEFT) && !checkKeyword(KeywordType::RIGHT) &&
               !checkKeyword(KeywordType::FULL) && !checkKeyword(KeywordType::CROSS) &&
               !checkKeyword(KeywordType::ON)) {
        item.alias = advance().text;
    }

    return item;
}

std::expected<FromClause, ParseError> Parser::parseFromClause() {
    consumeKeyword(KeywordType::FROM, "Expected FROM");

    FromClause clause;
    auto tableResult = parseTableRef();
    if (!tableResult) return std::unexpected(tableResult.error());
    clause.primary = std::move(*tableResult);

    while (checkKeyword(KeywordType::JOIN) || checkKeyword(KeywordType::INNER) ||
           checkKeyword(KeywordType::LEFT) || checkKeyword(KeywordType::RIGHT) ||
           checkKeyword(KeywordType::FULL) || checkKeyword(KeywordType::CROSS)) {
        auto joinResult = parseJoinClause();
        if (!joinResult) return std::unexpected(joinResult.error());
        clause.joins.push_back(std::move(*joinResult));
    }

    return clause;
}

std::expected<TableRef, ParseError> Parser::parseTableRef() {
    TableRef ref;

    if (checkDelimiter(DelimiterType::LeftParen)) {
        advance();
        auto subquery = parseSelectStatement();
        if (!subquery) return std::unexpected(subquery.error());
        auto closeResult = consumeDelimiter(DelimiterType::RightParen, "Expected )");
        if (!closeResult) return std::unexpected(closeResult.error());

        std::string alias;
        if (matchKeyword(KeywordType::AS)) {
            if (!check(TokenType::Identifier)) {
                return std::unexpected(error(peek(), "Expected alias"));
            }
            alias = advance().text;
        } else if (check(TokenType::Identifier)) {
            alias = advance().text;
        } else {
            return std::unexpected(error(peek(), "Subquery requires alias"));
        }

        TableRef::SubqueryTable subTable;
        subTable.query = std::move(*subquery);
        subTable.alias = std::move(alias);
        ref.source = std::move(subTable);
    } else {
        if (!check(TokenType::Identifier)) {
            return std::unexpected(error(peek(), "Expected table name"));
        }

        TableRef::BaseTable base;
        base.name = advance().text;

        if (matchDelimiter(DelimiterType::Dot)) {
            if (!check(TokenType::Identifier)) {
                return std::unexpected(error(peek(), "Expected table name after dot"));
            }
            base.schema = base.name;
            base.name = advance().text;
        }

        if (matchKeyword(KeywordType::AS)) {
            if (!check(TokenType::Identifier)) {
                return std::unexpected(error(peek(), "Expected alias"));
            }
            base.alias = advance().text;
        } else if (check(TokenType::Identifier) && !checkKeyword(KeywordType::JOIN) &&
                   !checkKeyword(KeywordType::INNER) && !checkKeyword(KeywordType::LEFT) &&
                   !checkKeyword(KeywordType::RIGHT) && !checkKeyword(KeywordType::FULL) &&
                   !checkKeyword(KeywordType::CROSS) && !checkKeyword(KeywordType::ON) &&
                   !checkKeyword(KeywordType::WHERE) && !checkKeyword(KeywordType::GROUP) &&
                   !checkKeyword(KeywordType::ORDER) && !checkKeyword(KeywordType::LIMIT) &&
                   !checkKeyword(KeywordType::HAVING) && !checkKeyword(KeywordType::UNION) &&
                   !checkKeyword(KeywordType::INTERSECT) && !checkKeyword(KeywordType::EXCEPT)) {
            base.alias = advance().text;
        }

        ref.source = std::move(base);
    }

    return ref;
}

std::expected<JoinClause, ParseError> Parser::parseJoinClause() {
    JoinClause join;

    if (matchKeyword(KeywordType::INNER)) {
        join.type = JoinType::Inner;
        consumeKeyword(KeywordType::JOIN, "Expected JOIN after INNER");
    } else if (matchKeyword(KeywordType::LEFT)) {
        join.type = JoinType::Left;
        matchKeyword(KeywordType::OUTER);
        consumeKeyword(KeywordType::JOIN, "Expected JOIN after LEFT");
    } else if (matchKeyword(KeywordType::RIGHT)) {
        join.type = JoinType::Right;
        matchKeyword(KeywordType::OUTER);
        consumeKeyword(KeywordType::JOIN, "Expected JOIN after RIGHT");
    } else if (matchKeyword(KeywordType::FULL)) {
        join.type = JoinType::Full;
        matchKeyword(KeywordType::OUTER);
        consumeKeyword(KeywordType::JOIN, "Expected JOIN after FULL");
    } else if (matchKeyword(KeywordType::CROSS)) {
        join.type = JoinType::Cross;
        consumeKeyword(KeywordType::JOIN, "Expected JOIN after CROSS");
    } else {
        consumeKeyword(KeywordType::JOIN, "Expected JOIN");
        join.type = JoinType::Inner;
    }

    auto tableResult = parseTableRef();
    if (!tableResult) return std::unexpected(tableResult.error());
    join.table = std::move(*tableResult);

    if (join.type != JoinType::Cross && matchKeyword(KeywordType::ON)) {
        auto condResult = parseExpression();
        if (!condResult) return std::unexpected(condResult.error());
        join.condition = std::move(*condResult);
    }

    return join;
}

std::expected<WhereClause, ParseError> Parser::parseWhereClause() {
    consumeKeyword(KeywordType::WHERE, "Expected WHERE");
    WhereClause clause;
    auto expr = parseExpression();
    if (!expr) return std::unexpected(expr.error());
    clause.condition = std::move(*expr);
    return clause;
}

std::expected<GroupByClause, ParseError> Parser::parseGroupByClause() {
    consumeKeyword(KeywordType::GROUP, "Expected GROUP");
    consumeKeyword(KeywordType::BY, "Expected BY after GROUP");

    GroupByClause clause;
    do {
        auto expr = parseExpression();
        if (!expr) return std::unexpected(expr.error());
        clause.expressions.push_back(std::move(*expr));
    } while (matchDelimiter(DelimiterType::Comma));

    return clause;
}

std::expected<HavingClause, ParseError> Parser::parseHavingClause() {
    consumeKeyword(KeywordType::HAVING, "Expected HAVING");
    HavingClause clause;
    auto expr = parseExpression();
    if (!expr) return std::unexpected(expr.error());
    clause.condition = std::move(*expr);
    return clause;
}

std::expected<OrderByClause, ParseError> Parser::parseOrderByClause() {
    consumeKeyword(KeywordType::ORDER, "Expected ORDER");
    consumeKeyword(KeywordType::BY, "Expected BY after ORDER");

    OrderByClause clause;
    do {
        OrderByItem item;
        auto expr = parseExpression();
        if (!expr) return std::unexpected(expr.error());
        item.expression = std::move(*expr);

        if (matchKeyword(KeywordType::ASC)) {
            item.ascending = true;
        } else if (matchKeyword(KeywordType::DESC)) {
            item.ascending = false;
        }

        clause.items.push_back(std::move(item));
    } while (matchDelimiter(DelimiterType::Comma));

    return clause;
}

std::expected<LimitClause, ParseError> Parser::parseLimitClause() {
    consumeKeyword(KeywordType::LIMIT, "Expected LIMIT");

    LimitClause clause;
    auto count = parseExpression();
    if (!count) return std::unexpected(count.error());
    clause.count = std::move(*count);

    if (matchKeyword(KeywordType::OFFSET)) {
        auto offset = parseExpression();
        if (!offset) return std::unexpected(offset.error());
        clause.offset = std::move(*offset);
    }

    return clause;
}

std::expected<ExprPtr, ParseError> Parser::parseExpression() {
    return parseOr();
}

std::expected<ExprPtr, ParseError> Parser::parseOr() {
    auto left = parseAnd();
    if (!left) return left;

    while (matchKeyword(KeywordType::OR)) {
        size_t line = previous().line;
        size_t col = previous().column;
        auto right = parseAnd();
        if (!right) return right;

        BinaryExpr binary;
        binary.left = std::move(*left);
        binary.op = OperatorType::Equal; // placeholder - we use KeywordType for logical ops
        binary.right = std::move(*right);
        binary.line = line;
        binary.column = col;

        auto newExpr = makeExpr(std::move(binary));
        std::get<BinaryExpr>(*newExpr).op = OperatorType::Equal;
        left = std::move(newExpr);
    }

    return left;
}

std::expected<ExprPtr, ParseError> Parser::parseAnd() {
    auto left = parseNot();
    if (!left) return left;

    while (matchKeyword(KeywordType::AND)) {
        size_t line = previous().line;
        size_t col = previous().column;
        auto right = parseNot();
        if (!right) return right;

        BinaryExpr binary;
        binary.left = std::move(*left);
        binary.op = OperatorType::Equal;
        binary.right = std::move(*right);
        binary.line = line;
        binary.column = col;
        left = makeExpr(std::move(binary));
    }

    return left;
}

std::expected<ExprPtr, ParseError> Parser::parseNot() {
    if (matchKeyword(KeywordType::NOT)) {
        size_t line = previous().line;
        size_t col = previous().column;
        auto operand = parseNot();
        if (!operand) return operand;

        UnaryExpr unary;
        unary.op = UnaryExpr::Op::Not;
        unary.operand = std::move(*operand);
        unary.line = line;
        unary.column = col;
        return makeExpr(std::move(unary));
    }
    return parseComparison();
}

std::expected<ExprPtr, ParseError> Parser::parseComparison() {
    auto left = parseAdditive();
    if (!left) return left;

    if (checkKeyword(KeywordType::BETWEEN)) {
        advance();
        bool negated = false;
        size_t line = previous().line;
        size_t col = previous().column;

        auto low = parseAdditive();
        if (!low) return low;
        consumeKeyword(KeywordType::AND, "Expected AND in BETWEEN");
        auto high = parseAdditive();
        if (!high) return high;

        BetweenExpr between;
        between.value = std::move(*left);
        between.low = std::move(*low);
        between.high = std::move(*high);
        between.negated = negated;
        between.line = line;
        between.column = col;
        return makeExpr(std::move(between));
    }

    if (checkKeyword(KeywordType::NOT)) {
        advance();
        if (checkKeyword(KeywordType::BETWEEN)) {
            advance();
            size_t line = previous().line;
            size_t col = previous().column;

            auto low = parseAdditive();
            if (!low) return low;
            consumeKeyword(KeywordType::AND, "Expected AND in BETWEEN");
            auto high = parseAdditive();
            if (!high) return high;

            BetweenExpr between;
            between.value = std::move(*left);
            between.low = std::move(*low);
            between.high = std::move(*high);
            between.negated = true;
            between.line = line;
            between.column = col;
            return makeExpr(std::move(between));
        } else if (checkKeyword(KeywordType::IN)) {
            advance();
            size_t line = previous().line;
            size_t col = previous().column;
            consumeDelimiter(DelimiterType::LeftParen, "Expected (");

            InExpr in;
            in.value = std::move(*left);
            in.negated = true;
            in.line = line;
            in.column = col;

            if (checkKeyword(KeywordType::SELECT)) {
                auto subquery = parseSelectStatement();
                if (!subquery) return std::unexpected(subquery.error());
                in.list = std::move(*subquery);
            } else {
                std::vector<ExprPtr> exprs;
                do {
                    auto expr = parseExpression();
                    if (!expr) return expr;
                    exprs.push_back(std::move(*expr));
                } while (matchDelimiter(DelimiterType::Comma));
                in.list = std::move(exprs);
            }

            consumeDelimiter(DelimiterType::RightParen, "Expected )");
            return makeExpr(std::move(in));
        } else if (checkKeyword(KeywordType::LIKE)) {
            advance();
            size_t line = previous().line;
            size_t col = previous().column;
            auto pattern = parseAdditive();
            if (!pattern) return pattern;

            LikeExpr like;
            like.value = std::move(*left);
            like.pattern = std::move(*pattern);
            like.negated = true;
            like.line = line;
            like.column = col;
            return makeExpr(std::move(like));
        }
        current_--;
    }

    if (checkKeyword(KeywordType::IN)) {
        advance();
        size_t line = previous().line;
        size_t col = previous().column;
        consumeDelimiter(DelimiterType::LeftParen, "Expected (");

        InExpr in;
        in.value = std::move(*left);
        in.negated = false;
        in.line = line;
        in.column = col;

        if (checkKeyword(KeywordType::SELECT)) {
            auto subquery = parseSelectStatement();
            if (!subquery) return std::unexpected(subquery.error());
            in.list = std::move(*subquery);
        } else {
            std::vector<ExprPtr> exprs;
            do {
                auto expr = parseExpression();
                if (!expr) return expr;
                exprs.push_back(std::move(*expr));
            } while (matchDelimiter(DelimiterType::Comma));
            in.list = std::move(exprs);
        }

        consumeDelimiter(DelimiterType::RightParen, "Expected )");
        return makeExpr(std::move(in));
    }

    if (checkKeyword(KeywordType::LIKE)) {
        advance();
        size_t line = previous().line;
        size_t col = previous().column;
        auto pattern = parseAdditive();
        if (!pattern) return pattern;

        LikeExpr like;
        like.value = std::move(*left);
        like.pattern = std::move(*pattern);
        like.negated = false;
        like.line = line;
        like.column = col;
        return makeExpr(std::move(like));
    }

    if (checkKeyword(KeywordType::IS)) {
        advance();
        bool negated = matchKeyword(KeywordType::NOT);
        size_t line = previous().line;
        size_t col = previous().column;

        if (!matchKeyword(KeywordType::NULL_KW)) {
            return std::unexpected(error(peek(), "Expected NULL after IS"));
        }

        IsNullExpr isNull;
        isNull.value = std::move(*left);
        isNull.negated = negated;
        isNull.line = line;
        isNull.column = col;
        return makeExpr(std::move(isNull));
    }

    while (check(TokenType::Operator)) {
        auto op = peek().operatorType;
        if (op == OperatorType::Equal || op == OperatorType::NotEqual ||
            op == OperatorType::LessThan || op == OperatorType::LessEqual ||
            op == OperatorType::GreaterThan || op == OperatorType::GreaterEqual) {
            advance();
            size_t line = previous().line;
            size_t col = previous().column;

            auto right = parseAdditive();
            if (!right) return right;

            BinaryExpr binary;
            binary.left = std::move(*left);
            binary.op = *op;
            binary.right = std::move(*right);
            binary.line = line;
            binary.column = col;
            left = makeExpr(std::move(binary));
        } else {
            break;
        }
    }

    return left;
}

std::expected<ExprPtr, ParseError> Parser::parseAdditive() {
    auto left = parseMultiplicative();
    if (!left) return left;

    while (check(TokenType::Operator)) {
        auto op = peek().operatorType;
        if (op == OperatorType::Plus || op == OperatorType::Minus || op == OperatorType::Concat) {
            advance();
            size_t line = previous().line;
            size_t col = previous().column;

            auto right = parseMultiplicative();
            if (!right) return right;

            BinaryExpr binary;
            binary.left = std::move(*left);
            binary.op = *op;
            binary.right = std::move(*right);
            binary.line = line;
            binary.column = col;
            left = makeExpr(std::move(binary));
        } else {
            break;
        }
    }

    return left;
}

std::expected<ExprPtr, ParseError> Parser::parseMultiplicative() {
    auto left = parseUnary();
    if (!left) return left;

    while (checkDelimiter(DelimiterType::Star) || check(TokenType::Operator)) {
        if (checkDelimiter(DelimiterType::Star)) {
            advance();
            size_t line = previous().line;
            size_t col = previous().column;

            auto right = parseUnary();
            if (!right) return right;

            BinaryExpr binary;
            binary.left = std::move(*left);
            binary.op = OperatorType::Multiply;
            binary.right = std::move(*right);
            binary.line = line;
            binary.column = col;
            left = makeExpr(std::move(binary));
        } else {
            auto op = peek().operatorType;
            if (op == OperatorType::Divide || op == OperatorType::Modulo) {
                advance();
                size_t line = previous().line;
                size_t col = previous().column;

                auto right = parseUnary();
                if (!right) return right;

                BinaryExpr binary;
                binary.left = std::move(*left);
                binary.op = *op;
                binary.right = std::move(*right);
                binary.line = line;
                binary.column = col;
                left = makeExpr(std::move(binary));
            } else {
                break;
            }
        }
    }

    return left;
}

std::expected<ExprPtr, ParseError> Parser::parseUnary() {
    if (check(TokenType::Operator) && peek().operatorType == OperatorType::Minus) {
        advance();
        size_t line = previous().line;
        size_t col = previous().column;

        auto operand = parseUnary();
        if (!operand) return operand;

        UnaryExpr unary;
        unary.op = UnaryExpr::Op::Minus;
        unary.operand = std::move(*operand);
        unary.line = line;
        unary.column = col;
        return makeExpr(std::move(unary));
    }

    return parsePrimary();
}

std::expected<ExprPtr, ParseError> Parser::parsePrimary() {
    if (check(TokenType::Literal)) {
        auto token = advance();
        LiteralExpr lit;
        lit.value = *token.literalValue;
        lit.line = token.line;
        lit.column = token.column;
        return makeExpr(std::move(lit));
    }

    if (checkKeyword(KeywordType::TRUE_KW)) {
        auto token = advance();
        LiteralExpr lit;
        lit.value = true;
        lit.line = token.line;
        lit.column = token.column;
        return makeExpr(std::move(lit));
    }

    if (checkKeyword(KeywordType::FALSE_KW)) {
        auto token = advance();
        LiteralExpr lit;
        lit.value = false;
        lit.line = token.line;
        lit.column = token.column;
        return makeExpr(std::move(lit));
    }

    if (checkKeyword(KeywordType::NULL_KW)) {
        auto token = advance();
        LiteralExpr lit;
        lit.value = Null{};
        lit.line = token.line;
        lit.column = token.column;
        return makeExpr(std::move(lit));
    }

    if (checkKeyword(KeywordType::CASE)) {
        return parseCaseExpression();
    }

    if (checkKeyword(KeywordType::CAST)) {
        return parseCastExpression();
    }

    if (checkKeyword(KeywordType::EXISTS)) {
        advance();
        size_t line = previous().line;
        size_t col = previous().column;
        consumeDelimiter(DelimiterType::LeftParen, "Expected (");
        auto subquery = parseSelectStatement();
        if (!subquery) return std::unexpected(subquery.error());
        consumeDelimiter(DelimiterType::RightParen, "Expected )");

        ExistsExpr exists;
        exists.query = std::move(*subquery);
        exists.line = line;
        exists.column = col;
        return makeExpr(std::move(exists));
    }

    if (checkDelimiter(DelimiterType::LeftParen)) {
        advance();
        if (checkKeyword(KeywordType::SELECT)) {
            auto subquery = parseSelectStatement();
            if (!subquery) return std::unexpected(subquery.error());
            consumeDelimiter(DelimiterType::RightParen, "Expected )");

            SubqueryExpr sub;
            sub.query = std::move(*subquery);
            return makeExpr(std::move(sub));
        }
        auto expr = parseExpression();
        if (!expr) return expr;
        consumeDelimiter(DelimiterType::RightParen, "Expected )");
        return expr;
    }

    if (check(TokenType::Identifier) || check(TokenType::Keyword)) {
        std::string name;
        if (check(TokenType::Keyword)) {
            auto kw = peek().keywordType;
            if (kw == KeywordType::COUNT || kw == KeywordType::SUM ||
                kw == KeywordType::AVG || kw == KeywordType::MIN ||
                kw == KeywordType::MAX) {
                name = advance().text;
                std::transform(name.begin(), name.end(), name.begin(), ::toupper);
            } else {
                return std::unexpected(error(peek(), "Unexpected keyword in expression"));
            }
        } else {
            name = advance().text;
        }

        if (checkDelimiter(DelimiterType::LeftParen)) {
            return parseFunctionCall(name);
        }

        IdentifierExpr ident;
        ident.parts.push_back(name);
        ident.line = previous().line;
        ident.column = previous().column;

        while (matchDelimiter(DelimiterType::Dot)) {
            if (!check(TokenType::Identifier)) {
                return std::unexpected(error(peek(), "Expected identifier after dot"));
            }
            ident.parts.push_back(advance().text);
        }

        return makeExpr(std::move(ident));
    }

    return std::unexpected(error(peek(), "Expected expression"));
}

std::expected<ExprPtr, ParseError> Parser::parseFunctionCall(const std::string& name) {
    consumeDelimiter(DelimiterType::LeftParen, "Expected (");

    FunctionCallExpr func;
    func.name = name;
    func.line = previous().line;
    func.column = previous().column;

    if (matchKeyword(KeywordType::DISTINCT)) {
        func.distinct = true;
    }

    if (!checkDelimiter(DelimiterType::RightParen)) {
        if (checkDelimiter(DelimiterType::Star)) {
            advance();
            StarExpr star;
            star.line = previous().line;
            star.column = previous().column;
            func.arguments.push_back(makeExpr(std::move(star)));
        } else {
            do {
                auto arg = parseExpression();
                if (!arg) return arg;
                func.arguments.push_back(std::move(*arg));
            } while (matchDelimiter(DelimiterType::Comma));
        }
    }

    consumeDelimiter(DelimiterType::RightParen, "Expected )");
    return makeExpr(std::move(func));
}

std::expected<ExprPtr, ParseError> Parser::parseCaseExpression() {
    consumeKeyword(KeywordType::CASE, "Expected CASE");

    CaseExpr caseExpr;
    caseExpr.line = previous().line;
    caseExpr.column = previous().column;

    if (!checkKeyword(KeywordType::WHEN)) {
        auto operand = parseExpression();
        if (!operand) return operand;
        caseExpr.operand = std::move(*operand);
    }

    while (matchKeyword(KeywordType::WHEN)) {
        CaseExpr::WhenClause when;
        auto cond = parseExpression();
        if (!cond) return cond;
        when.condition = std::move(*cond);

        consumeKeyword(KeywordType::THEN, "Expected THEN");
        auto result = parseExpression();
        if (!result) return result;
        when.result = std::move(*result);

        caseExpr.whenClauses.push_back(std::move(when));
    }

    if (matchKeyword(KeywordType::ELSE)) {
        auto elseExpr = parseExpression();
        if (!elseExpr) return elseExpr;
        caseExpr.elseClause = std::move(*elseExpr);
    }

    consumeKeyword(KeywordType::END, "Expected END");
    return makeExpr(std::move(caseExpr));
}

std::expected<ExprPtr, ParseError> Parser::parseCastExpression() {
    consumeKeyword(KeywordType::CAST, "Expected CAST");
    consumeDelimiter(DelimiterType::LeftParen, "Expected (");

    auto value = parseExpression();
    if (!value) return value;

    consumeKeyword(KeywordType::AS, "Expected AS");

    if (!check(TokenType::Keyword)) {
        return std::unexpected(error(peek(), "Expected type name"));
    }

    auto typeToken = advance();
    auto targetType = typeToken.keywordType;
    if (!targetType || (*targetType != KeywordType::INT && *targetType != KeywordType::INTEGER &&
                        *targetType != KeywordType::BIGINT && *targetType != KeywordType::FLOAT &&
                        *targetType != KeywordType::DOUBLE && *targetType != KeywordType::DECIMAL &&
                        *targetType != KeywordType::TEXT && *targetType != KeywordType::BOOLEAN &&
                        *targetType != KeywordType::DATE && *targetType != KeywordType::TIME &&
                        *targetType != KeywordType::TIMESTAMP)) {
        return std::unexpected(error(typeToken, "Invalid type for CAST"));
    }

    consumeDelimiter(DelimiterType::RightParen, "Expected )");

    CastExpr cast;
    cast.value = std::move(*value);
    cast.targetType = *targetType;
    cast.line = typeToken.line;
    cast.column = typeToken.column;
    return makeExpr(std::move(cast));
}

const Token& Parser::peek() const {
    return tokens_[current_];
}

const Token& Parser::previous() const {
    return tokens_[current_ - 1];
}

Token Parser::advance() {
    if (!isAtEnd()) current_++;
    return previous();
}

bool Parser::check(TokenType type) const {
    if (isAtEnd()) return false;
    return peek().type == type;
}

bool Parser::checkKeyword(KeywordType kw) const {
    if (isAtEnd()) return false;
    return peek().type == TokenType::Keyword && peek().keywordType == kw;
}

bool Parser::checkDelimiter(DelimiterType d) const {
    if (isAtEnd()) return false;
    return peek().type == TokenType::Delimiter && peek().delimiterType == d;
}

bool Parser::match(TokenType type) {
    if (check(type)) {
        advance();
        return true;
    }
    return false;
}

bool Parser::matchKeyword(KeywordType kw) {
    if (checkKeyword(kw)) {
        advance();
        return true;
    }
    return false;
}

bool Parser::matchDelimiter(DelimiterType d) {
    if (checkDelimiter(d)) {
        advance();
        return true;
    }
    return false;
}

std::expected<Token, ParseError> Parser::consume(TokenType type, std::string_view message) {
    if (check(type)) return advance();
    return std::unexpected(error(peek(), std::string(message)));
}

std::expected<Token, ParseError> Parser::consumeKeyword(KeywordType kw, std::string_view message) {
    if (checkKeyword(kw)) return advance();
    return std::unexpected(error(peek(), std::string(message)));
}

std::expected<Token, ParseError> Parser::consumeDelimiter(DelimiterType d, std::string_view message) {
    if (checkDelimiter(d)) return advance();
    return std::unexpected(error(peek(), std::string(message)));
}

bool Parser::isAtEnd() const {
    return current_ >= tokens_.size() || peek().type == TokenType::EndOfFile;
}

ParseError Parser::error(const Token& token, std::string message) const {
    ParseError err;
    err.message = std::move(message);
    err.location = {token.line, token.column};
    err.snippet = token.text;
    return err;
}
