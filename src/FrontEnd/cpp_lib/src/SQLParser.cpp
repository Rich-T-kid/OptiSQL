#include "SQLParser.hpp"

namespace optisql {

std::expected<ParseResult, ParseError> SQLParser::parse(const std::string& sql, bool optimize) {
    lexer_ = Lexer(sql);
    auto tokens = lexer_.tokenize();

    if (!tokens) {
        return std::unexpected(tokens.error());
    }

    parser_ = Parser(*tokens);
    auto stmt = parser_.parse();

    if (!stmt) {
        return std::unexpected(stmt.error());
    }

    SelectStatement result = std::move(**stmt);

    if (optimize) {
        result = optimizer_.optimize(std::move(result));
    }

    ParseResult parseResult;
    parseResult.ast = serializer_.serialize(result);
    parseResult.optimized = optimize;

    return parseResult;
}

std::string SQLParser::parseToJson(const std::string& sql, bool optimize) {
    auto result = parse(sql, optimize);

    if (!result) {
        json errorJson;
        errorJson["error"] = true;
        errorJson["message"] = result.error().message;
        errorJson["line"] = result.error().location.line;
        errorJson["column"] = result.error().location.column;
        return errorJson.dump();
    }

    return result->ast.dump();
}

} // namespace optisql
